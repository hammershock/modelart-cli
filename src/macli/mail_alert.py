"""Email alerts for macli disk monitoring."""
import smtplib
import time
from email.message import EmailMessage

from macli.config import load_alert_email_cfg, save_alert_email_cfg


DEFAULT_THROTTLE_HOURS = 12
ALLOC_THRESHOLDS = (50, 70, 85)
SHARE_THRESHOLDS = (10, 30, 50)


def default_alert_email_cfg() -> dict:
    return {
        "enabled": False,
        "recipients": [],
        "throttle_hours": DEFAULT_THROTTLE_HOURS,
        "smtp": {},
        "last_disk_alert_ts": 0,
    }


def merged_alert_email_cfg() -> dict:
    cfg = default_alert_email_cfg()
    saved = load_alert_email_cfg() or {}
    cfg.update(saved)
    cfg["recipients"] = list(dict.fromkeys(cfg.get("recipients") or []))
    cfg["smtp"] = dict(cfg.get("smtp") or {})
    cfg["throttle_hours"] = float(cfg.get("throttle_hours") or DEFAULT_THROTTLE_HOURS)
    return cfg


def save_merged_alert_email_cfg(cfg: dict):
    base = default_alert_email_cfg()
    base.update(cfg or {})
    base["recipients"] = list(dict.fromkeys(base.get("recipients") or []))
    base["smtp"] = dict(base.get("smtp") or {})
    save_alert_email_cfg(base)


def disk_level(value, thresholds: tuple) -> str:
    if value is None:
        return "unknown"
    yellow, orange, red = thresholds
    if value >= red:
        return "red"
    if value >= orange:
        return "orange"
    if value >= yellow:
        return "yellow"
    return "green"


def fmt_bytes(num) -> str:
    if num is None:
        return "-"
    sign = "-" if num < 0 else ""
    n = abs(float(num))
    for factor, suffix in (
        (1024 ** 4, "TiB"),
        (1024 ** 3, "GiB"),
        (1024 ** 2, "MiB"),
        (1024, "KiB"),
    ):
        if n >= factor:
            value = n / factor
            digits = 1 if value < 10 else 0
            text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
            return f"{sign}{text}{suffix}"
    return f"{sign}{int(n)}B"


def fmt_pct(value) -> str:
    return f"{value:.2f}%" if value is not None else "-"


def disk_risk_jobs(disk_state: dict) -> list:
    risks = []
    for host in ((disk_state or {}).get("hosts") or {}).values():
        df = host.get("df") or {}
        used = int(df.get("used_bytes") or 0)
        total = int(df.get("total_bytes") or 0)
        evict_limit = int(total * 0.9)
        alloc_pct = used / evict_limit * 100 if evict_limit > 0 else None
        alloc_level = disk_level(alloc_pct, ALLOC_THRESHOLDS)
        margin_bytes = evict_limit - used if evict_limit > 0 else None
        for job in host.get("jobs", []) or []:
            cache = job.get("cache_bytes")
            share_pct = int(cache) / used * 100 if cache is not None and used > 0 else None
            share_level = disk_level(share_pct, SHARE_THRESHOLDS)
            critical = alloc_level == "red" and share_level == "red"
            warning = {alloc_level, share_level} == {"red", "orange"}
            if not (critical or warning):
                continue
            risks.append({
                "severity": "critical" if critical else "warning",
                "host_ip": host.get("host_ip") or "UNKNOWN",
                "job_id": job.get("id") or "",
                "name": job.get("name") or "",
                "port": job.get("port"),
                "alloc_pct": alloc_pct,
                "share_pct": share_pct,
                "margin_bytes": margin_bytes,
                "cache_bytes": cache,
                "used_bytes": used,
                "evict_limit_bytes": evict_limit,
            })
    return sorted(
        risks,
        key=lambda r: (
            0 if r["severity"] == "critical" else 1,
            -(r.get("alloc_pct") or 0),
            -(r.get("share_pct") or 0),
        ),
    )


def _smtp_ready(cfg: dict) -> bool:
    smtp = cfg.get("smtp") or {}
    return bool(
        smtp.get("host") and smtp.get("port") and
        smtp.get("username") and smtp.get("password") and
        (smtp.get("from_email") or smtp.get("username"))
    )


def send_email(subject: str, body: str, recipients=None, cfg: dict = None):
    cfg = cfg or merged_alert_email_cfg()
    smtp = cfg.get("smtp") or {}
    recipients = list(dict.fromkeys(recipients or cfg.get("recipients") or []))
    if not recipients:
        raise RuntimeError("no alert email recipients configured")
    if not _smtp_ready(cfg):
        raise RuntimeError("smtp is not fully configured")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp.get("from_email") or smtp.get("username")
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    host = smtp["host"]
    port = int(smtp["port"])
    security = (smtp.get("security") or "ssl").lower()
    username = smtp["username"]
    password = smtp["password"]
    timeout = int(smtp.get("timeout") or 30)

    if security == "ssl":
        client = smtplib.SMTP_SSL(host, port, timeout=timeout)
    else:
        client = smtplib.SMTP(host, port, timeout=timeout)
    try:
        if security == "starttls":
            client.starttls()
        client.login(username, password)
        client.send_message(msg)
    finally:
        try:
            client.quit()
        except Exception:
            pass


def disk_alert_body(risks: list, disk_state: dict) -> str:
    lines = [
        "macli disk eviction risk alert",
        "",
        f"Snapshot: {(disk_state or {}).get('last_check', '-')}",
        f"Risk jobs: {len(risks)}",
        "",
    ]
    for r in risks:
        lines.append(
            f"[{r['severity'].upper()}] "
            f"port={r.get('port') or '-'} "
            f"job={r.get('job_id', '')[:8]} "
            f"name={r.get('name') or '-'} "
            f"host={r.get('host_ip') or '-'} "
            f"alloc={fmt_pct(r.get('alloc_pct'))} "
            f"share={fmt_pct(r.get('share_pct'))} "
            f"margin={fmt_bytes(r.get('margin_bytes'))} "
            f"cache={fmt_bytes(r.get('cache_bytes'))}"
        )
    lines += [
        "",
        "Allocation >= 100% is sufficient to trigger eviction.",
        "Eviction targets the JOB with the largest current share; share is this JOB's cache usage divided by allocated space.",
        "Other JOBs may exist and actual usage may be unknown, so share is only a decision aid.",
    ]
    return "\n".join(lines) + "\n"


def send_disk_alert_if_needed(disk_state: dict, force: bool = False) -> tuple:
    cfg = merged_alert_email_cfg()
    if not cfg.get("enabled"):
        return False, "alert email disabled", []
    risks = disk_risk_jobs(disk_state)
    if not risks:
        return False, "no red/red or red/orange disk risks", []
    if not cfg.get("recipients"):
        return False, "no alert email recipients configured", risks
    if not _smtp_ready(cfg):
        return False, "smtp is not fully configured", risks

    now = time.time()
    throttle_s = float(cfg.get("throttle_hours") or DEFAULT_THROTTLE_HOURS) * 3600
    last = float(cfg.get("last_disk_alert_ts") or 0)
    if not force and last > 0 and now - last < throttle_s:
        remain_h = (throttle_s - (now - last)) / 3600
        return False, f"throttled for {remain_h:.1f}h", risks

    critical = sum(1 for r in risks if r["severity"] == "critical")
    warning = len(risks) - critical
    subject = f"[macli] Disk eviction risk: {critical} critical, {warning} warning"
    send_email(subject, disk_alert_body(risks, disk_state), cfg=cfg)
    cfg["last_disk_alert_ts"] = now
    cfg["last_disk_alert_count"] = len(risks)
    save_merged_alert_email_cfg(cfg)
    return True, "sent", risks
