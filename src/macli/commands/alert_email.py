"""macli alert-email -- manage email alerts."""
import json
import sys
from pathlib import Path

from rich.table import Table

from macli.constants import console
from macli.log import cprint
from macli.mail_alert import (
    DEFAULT_THROTTLE_HOURS,
    disk_risk_jobs,
    merged_alert_email_cfg,
    save_merged_alert_email_cfg,
    send_disk_alert_if_needed,
    send_email,
)


def _mask(value: str) -> str:
    if not value:
        return ""
    if "@" in value:
        name, domain = value.split("@", 1)
        if len(name) <= 2:
            return f"{name[:1]}***@{domain}"
        return f"{name[:2]}***@{domain}"
    if len(value) <= 4:
        return "***"
    return value[:2] + "***" + value[-2:]


def _parse_scalar(value: str):
    value = value.strip()
    if not value:
        return ""
    if value[0:1] in ("'", '"') and value[-1:] == value[0]:
        value = value[1:-1]
    if value.isdigit():
        return int(value)
    return value


def _simple_mail_accounts_yaml(path: Path) -> list:
    accounts = []
    cur = None
    section = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        if indent == 0:
            continue
        if line.startswith("- "):
            cur = {}
            accounts.append(cur)
            section = None
            line = line[2:].strip()
            if ":" in line:
                k, v = line.split(":", 1)
                cur[k.strip()] = _parse_scalar(v)
            continue
        if cur is None or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if indent <= 4 and not value:
            section = key
            cur.setdefault(section, {})
        elif indent <= 4:
            cur[key] = _parse_scalar(value)
            section = None
        elif section:
            cur.setdefault(section, {})[key] = _parse_scalar(value)
    return accounts


def _load_mail_accounts(path: Path) -> list:
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return data.get("accounts") or []
    except Exception:
        return _simple_mail_accounts_yaml(path)


def _pick_account(accounts: list, provider: str = None, email: str = None) -> dict:
    provider_l = (provider or "").lower()
    email_l = (email or "").lower()
    for account in accounts:
        acc_email = str(account.get("email") or "").lower()
        acc_provider = str(account.get("provider") or "").lower()
        if email_l and acc_email == email_l:
            return account
        if provider_l and (provider_l == acc_provider or provider_l in acc_provider):
            return account
        if provider_l and provider_l in acc_email:
            return account
    return {}


def _password_from_account(account: dict) -> str:
    creds = account.get("credentials") or {}
    for key in ("smtp_auth_code", "app_password", "password"):
        if creds.get(key):
            return str(creds[key])
    return ""


def _show_status(cfg: dict):
    smtp = cfg.get("smtp") or {}
    cprint(f"提醒邮箱：[{'green' if cfg.get('enabled') else 'dim'}]{'enabled' if cfg.get('enabled') else 'disabled'}[/]")
    cprint(f"  throttle : {cfg.get('throttle_hours', DEFAULT_THROTTLE_HOURS)}h")
    cprint(f"  smtp     : {smtp.get('host', '—')}:{smtp.get('port', '—')} {smtp.get('security', '—')}")
    if smtp.get("username"):
        cprint(f"  user     : {_mask(smtp.get('username'))}")
    if smtp.get("from_email"):
        cprint(f"  from     : {_mask(smtp.get('from_email'))}")
    recipients = cfg.get("recipients") or []
    if recipients:
        cprint("  recipients:")
        for email in recipients:
            cprint(f"    - {_mask(email)}")
    else:
        cprint("  recipients: [dim](none)[/dim]")
    if cfg.get("last_disk_alert_ts"):
        cprint(f"  last disk alert ts: [dim]{cfg.get('last_disk_alert_ts')}[/dim]")


def cmd_alert_email(args):
    cfg = merged_alert_email_cfg()
    action = args.alert_email_action

    if action == "status":
        _show_status(cfg)
        return

    if action == "list":
        recipients = cfg.get("recipients") or []
        if not recipients:
            cprint("[dim]暂无提醒邮箱[/dim]")
            return
        t = Table(header_style="bold cyan", show_lines=False)
        t.add_column("email")
        for email in recipients:
            t.add_row(email)
        console.print(t)
        return

    if action == "add":
        recipients = cfg.setdefault("recipients", [])
        if args.email not in recipients:
            recipients.append(args.email)
        save_merged_alert_email_cfg(cfg)
        cprint(f"[green]✓ 已添加提醒邮箱：{args.email}[/green]")
        return

    if action == "remove":
        recipients = cfg.get("recipients") or []
        if args.email not in recipients:
            cprint(f"[red]未找到提醒邮箱：{args.email}[/red]")
            sys.exit(1)
        cfg["recipients"] = [e for e in recipients if e != args.email]
        save_merged_alert_email_cfg(cfg)
        cprint(f"[green]✓ 已移除提醒邮箱：{args.email}[/green]")
        return

    if action == "enable":
        cfg["enabled"] = True
        save_merged_alert_email_cfg(cfg)
        cprint("[green]✓ 邮件提醒已启用[/green]")
        return

    if action == "disable":
        cfg["enabled"] = False
        save_merged_alert_email_cfg(cfg)
        cprint("[green]✓ 邮件提醒已停用[/green]")
        return

    if action == "smtp":
        smtp = dict(cfg.get("smtp") or {})
        for key in ("host", "port", "username", "password", "security", "from_email"):
            value = getattr(args, key, None)
            if value is not None:
                smtp[key] = value
        cfg["smtp"] = smtp
        if args.throttle_hours is not None:
            cfg["throttle_hours"] = args.throttle_hours
        if args.enable:
            cfg["enabled"] = True
        save_merged_alert_email_cfg(cfg)
        cprint("[green]✓ SMTP 配置已更新[/green]")
        return

    if action == "import-yaml":
        accounts = _load_mail_accounts(Path(args.path).expanduser())
        account = _pick_account(accounts, provider=args.provider, email=args.email)
        if not account:
            cprint("[red]未在 YAML 中找到匹配的邮箱账号[/red]")
            sys.exit(1)
        smtp_in = account.get("smtp") or {}
        password = _password_from_account(account)
        if not password:
            cprint("[red]YAML 中未找到 SMTP 授权码/密码[/red]")
            sys.exit(1)
        cfg["smtp"] = {
            "host": smtp_in.get("host"),
            "port": int(smtp_in.get("port") or 465),
            "security": smtp_in.get("security") or "ssl",
            "username": smtp_in.get("username") or account.get("email"),
            "password": password,
            "from_email": account.get("email") or smtp_in.get("username"),
        }
        if args.throttle_hours is not None:
            cfg["throttle_hours"] = args.throttle_hours
        recipients = list(cfg.get("recipients") or [])
        for email in (args.recipient or [account.get("email")]):
            if email and email not in recipients:
                recipients.append(email)
        cfg["recipients"] = recipients
        if args.enable:
            cfg["enabled"] = True
        save_merged_alert_email_cfg(cfg)
        cprint(f"[green]✓ 已导入 SMTP：{_mask(cfg['smtp'].get('username'))}[/green]")
        cprint(f"[green]✓ 提醒邮箱：{', '.join(_mask(e) for e in recipients) or '—'}[/green]")
        return

    if action == "test":
        recipients = args.to or cfg.get("recipients") or []
        send_email(
            "[macli] test alert email",
            "This is a macli alert email test.\n",
            recipients=recipients,
            cfg=cfg,
        )
        cprint(f"[green]✓ 测试邮件已发送：{', '.join(_mask(e) for e in recipients)}[/green]")
        return

    if action == "check-disk":
        path = Path(args.snapshot).expanduser()
        if not path.exists():
            cprint(f"[red]磁盘快照不存在：{path}[/red]")
            sys.exit(1)
        disk_state = json.loads(path.read_text(encoding="utf-8"))
        risks = disk_risk_jobs(disk_state)
        if args.json:
            print(json.dumps(risks, ensure_ascii=False, indent=2))
            return
        sent, reason, risks = send_disk_alert_if_needed(disk_state, force=args.force)
        if sent:
            cprint(f"[green]✓ 已发送磁盘告警邮件：{len(risks)} 个风险作业[/green]")
        else:
            cprint(f"[yellow]未发送：{reason}（风险作业 {len(risks)} 个）[/yellow]")
        return
