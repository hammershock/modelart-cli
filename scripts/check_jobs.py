#!/usr/bin/env python3
"""
macli watch 检查脚本
--------------------
每次运行执行两项检查：

1. 无 Pending 作业时，从 Running 作业复制一个新作业
   - GPU 卡数固定为 1
   - 启动脚本使用本目录下的 startup.sh
   - 名称随机生成

2. 追踪 Terminated/Stopped/Failed 作业的终止时刻
   - 首次发现时记录当前时刻
   - 超过阈值（默认 72h）后自动删除该作业

由 `macli watch enable` 管理，也可独立运行：
    python scripts/check_jobs.py [--threshold-hours N]
"""
import os
import sys
import json
import shutil
import random
import string
import subprocess
import argparse
import concurrent.futures
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 路径配置 ──────────────────────────────────────────────────
SCRIPTS_DIR    = Path(__file__).parent.resolve()
STARTUP_SCRIPT = SCRIPTS_DIR / "startup.sh"
CONFIG_DIR     = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli"
STATE_FILE     = CONFIG_DIR / "watch_state.json"
DISK_STATE_FILE = CONFIG_DIR / "disk_state.json"
LOG_FILE       = CONFIG_DIR / "watch.log"

DEFAULT_THRESHOLD_H = 72

PENDING_PHASES    = {"Pending", "Waiting"}
RUNNING_PHASE     = "Running"
TERMINATED_PHASES = {"Stopped", "Terminated", "Failed"}

# ── 日志 ─────────────────────────────────────────────────────
def _log(level: str, msg: str):
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{level}: {ts}: {msg}"
    print(line, flush=True)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass

def info(msg: str):  _log("INFO",  msg)
def warn(msg: str):  _log("WARN",  msg)
def error(msg: str): _log("ERROR", msg)

# ── 状态存储 ──────────────────────────────────────────────────
def load_state() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"terminated_times": {}}

def save_state(state: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8"
    )

# ── macli 调用 ────────────────────────────────────────────────
def _macli_cmd() -> list:
    """返回调用 macli 的命令前缀（优先用 PATH，其次用当前 Python -m macli）。"""
    exe = shutil.which("macli")
    if exe:
        return [exe]
    return [sys.executable, "-m", "macli"]

def macli(*args) -> tuple:
    """运行 macli 命令，返回 (stdout, returncode)。失败时记录错误输出。"""
    cmd    = _macli_cmd() + [str(a) for a in args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        # stdout 含有 Rich 格式的错误消息（如 "创建失败 400: ..."）
        out_snippet = result.stdout.strip()[:400]
        err_snippet = result.stderr.strip()[:200]
        if out_snippet:
            error(f"macli output: {out_snippet}")
        if err_snippet:
            error(f"macli stderr: {err_snippet}")
    return result.stdout.strip(), result.returncode

def macli_exec_stdin(job_id: str, script: str, timeout: int = 180) -> tuple:
    """Run a shell script inside a job through macli exec ssh backend."""
    cmd = _macli_cmd() + [
        "exec", job_id,
        "--timeout", str(timeout),
        "--stdin",
    ]
    try:
        result = subprocess.run(
            cmd,
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout + 30,
        )
        return result.stdout.strip(), result.returncode
    except subprocess.TimeoutExpired:
        return "macli exec timeout", 124

def get_jobs() -> list:
    """获取所有作业（id / name / status），解析 macli query --json 输出。"""
    out, code = macli("query", "--json")
    if code != 0 or not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        error(f"JSON 解析失败: {out[:200]}")
        return []

def get_ports() -> list:
    """Return running jobs with SSH port metadata, refreshed from job detail."""
    out, code = macli("ports", "--refresh", "--json")
    if code != 0 or not out:
        return []
    try:
        data = json.loads(out)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        error(f"ports JSON 解析失败: {out[:200]}")
        return []

def fmt_bytes(num) -> str:
    try:
        n = float(num)
    except (TypeError, ValueError):
        return "—"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or unit == "PiB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024

def parse_disk_probe(text: str) -> dict:
    result = {"df": None, "du": None, "raw": text[-1000:]}
    for line in text.splitlines():
        parts = line.strip().split("\t")
        if not parts:
            continue
        if parts[0] == "DF" and len(parts) >= 7:
            try:
                total = int(parts[2])
                used = int(parts[3])
                avail = int(parts[4])
            except ValueError:
                continue
            result["df"] = {
                "filesystem": parts[1],
                "total_bytes": total,
                "used_bytes": used,
                "avail_bytes": avail,
                "used_pct": parts[5],
                "mount": parts[6],
                "total": fmt_bytes(total),
                "used": fmt_bytes(used),
                "avail": fmt_bytes(avail),
            }
        elif parts[0] == "DU" and len(parts) >= 3:
            try:
                size = int(float(parts[1]))
            except ValueError:
                continue
            result["du"] = {
                "cache_bytes": size,
                "cache": fmt_bytes(size),
                "path": parts[2],
            }
    return result

def probe_job_cache_disk(target: dict) -> dict:
    """Probe df /cache and du /cache from one running job container."""
    script = r"""
df -B1 /cache 2>/dev/null | awk 'NR==2 {print "DF\t"$1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6}'
du_line=$(du -sb /cache 2>/dev/null | awk 'NR==1 {print $1 "\t" $2}')
if [ -n "$du_line" ]; then
  printf 'DU\t%s\n' "$du_line"
else
  du -sk /cache 2>/dev/null | awk 'NR==1 {print "DU\t"($1*1024)"\t"$2}'
fi
"""
    out, code = macli_exec_stdin(target["id"], script, timeout=180)
    parsed = parse_disk_probe(out)
    ok = code == 0 and parsed.get("df") is not None and parsed.get("du") is not None
    return {
        **target,
        "ok": ok,
        "returncode": code,
        "error": "" if ok else out[-500:],
        "df": parsed.get("df"),
        "du": parsed.get("du"),
    }

def collect_disk_state(max_workers: int = 4):
    """Collect per-host /cache df and per-job /cache du, overwriting prior state."""
    started = datetime.now(timezone.utc)
    ports = get_ports()
    targets = []
    for job in ports:
        for ssh in job.get("ssh", []) or []:
            if ssh.get("port") is None:
                continue
            targets.append({
                "id": job.get("id", ""),
                "name": job.get("name", ""),
                "task": ssh.get("task", ""),
                "port": ssh.get("port"),
                "pod_ip": ssh.get("pod_ip", ""),
                "host_ip": ssh.get("host_ip", "") or "UNKNOWN",
            })

    state = {
        "last_check": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "jobs_checked": len(targets),
        "hosts": {},
        "errors": [],
    }
    if not targets:
        state["errors"].append("no running jobs with ssh ports")
        save_disk_state(state)
        warn("磁盘监控：没有可检查的 Running SSH 作业")
        return state

    # Avoid concurrent macli exec processes racing on session.json by setting
    # the remembered backend once before launching probes.
    macli("exec", "--backend", "ssh")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(probe_job_cache_disk, t) for t in targets]
        for fut in concurrent.futures.as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:
                state["errors"].append(f"probe failed: {type(exc).__name__}: {exc}")

    for item in sorted(results, key=lambda r: (r.get("host_ip", ""), r.get("port") or 0)):
        host_ip = item.get("host_ip") or "UNKNOWN"
        host = state["hosts"].setdefault(host_ip, {
            "host_ip": host_ip,
            "df": None,
            "jobs": [],
            "total_cache_bytes": 0,
            "total_cache": "0B",
            "errors": [],
        })
        if item.get("df") and host["df"] is None:
            host["df"] = item["df"]
        du = item.get("du") or {}
        cache_bytes = int(du.get("cache_bytes") or 0)
        if item.get("ok"):
            host["total_cache_bytes"] += cache_bytes
        else:
            host["errors"].append({
                "port": item.get("port"),
                "job": item.get("name"),
                "error": item.get("error", ""),
            })
        host["jobs"].append({
            "id": item.get("id"),
            "name": item.get("name"),
            "task": item.get("task"),
            "port": item.get("port"),
            "pod_ip": item.get("pod_ip"),
            "host_ip": host_ip,
            "cache_bytes": cache_bytes if item.get("ok") else None,
            "cache": fmt_bytes(cache_bytes) if item.get("ok") else "—",
            "ok": bool(item.get("ok")),
            "error": item.get("error", ""),
        })

    for host in state["hosts"].values():
        host["total_cache"] = fmt_bytes(host["total_cache_bytes"])

    save_disk_state(state)
    ok_count = sum(1 for r in results if r.get("ok"))
    info(f"磁盘监控完成：hosts={len(state['hosts'])} jobs={ok_count}/{len(targets)}")
    return state

def save_disk_state(state: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    DISK_STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8"
    )

def maybe_send_disk_alert(disk_state: dict):
    if not disk_state:
        return
    try:
        from macli.mail_alert import send_disk_alert_if_needed
        sent, reason, risks = send_disk_alert_if_needed(disk_state)
        if sent:
            info(f"磁盘告警邮件已发送：风险作业 {len(risks)} 个")
        elif risks:
            info(f"磁盘告警邮件未发送：{reason}（风险作业 {len(risks)} 个）")
    except Exception as exc:
        error(f"磁盘告警邮件失败：{type(exc).__name__}: {exc}")

# ── 检查逻辑 ──────────────────────────────────────────────────
def check(threshold_hours: int = DEFAULT_THRESHOLD_H):
    now   = datetime.now(timezone.utc)
    disk_state = None

    try:
        disk_state = collect_disk_state()
    except Exception as exc:
        error(f"磁盘监控失败：{type(exc).__name__}: {exc}")

    state = load_state()
    jobs  = get_jobs()

    if not jobs and not state.get("terminated_times"):
        warn("无法获取作业列表，跳过本次检查")
        maybe_send_disk_alert(disk_state)
        return

    pending    = [j for j in jobs if j.get("status", "") in PENDING_PHASES]
    running    = [j for j in jobs if j.get("status", "") == RUNNING_PHASE]
    terminated = [j for j in jobs if j.get("status", "") in TERMINATED_PHASES]

    info(f"作业统计：pending={len(pending)} running={len(running)} terminated={len(terminated)}")

    # ── 规则一：无 Pending → 复制一个 Running 作业 ───────────
    if not pending:
        if not running:
            warn("既无 Pending 也无 Running 作业，无法复制")
        else:
            src      = running[0]
            suffix   = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
            new_name = f"keeper-{suffix}"

            # 用 --json 确保能解析结果，且跳过交互预览
            if STARTUP_SCRIPT.exists():
                copy_args = [
                    "copy", src["id"],
                    "--gpu-count", "1",
                    "--name",      new_name,
                    "--desc",      "自动保活作业 macli watch",
                    "--command-file", str(STARTUP_SCRIPT),
                    "--json", "--yes",
                ]
            else:
                warn(f"未找到 startup.sh（{STARTUP_SCRIPT}），使用默认 sleep 命令")
                copy_args = [
                    "copy", src["id"],
                    "--gpu-count", "1",
                    "--name",      new_name,
                    "--desc",      "自动保活作业 macli watch",
                    "--command",   "sleep 2000000000s;",
                    "--json", "--yes",
                ]

            out, code = macli(*copy_args)
            if code == 0:
                try:
                    created = json.loads(out)
                    new_id  = created.get("id", "?")
                    phase   = created.get("status", "?")
                    info(f"已复制：{src['name']} → {new_name} (id={new_id[:8]}… phase={phase})")
                except (json.JSONDecodeError, AttributeError):
                    info(f"已复制：{src['name']} → {new_name}（无法解析返回 JSON）")
            else:
                error(f"复制失败（源：{src['name']} / {src['id'][:8]}…）")
    else:
        info(f"已有 {len(pending)} 个 Pending 作业，跳过复制")

    # ── 规则二：追踪 Terminated，超时删除 ────────────────────
    term_times: dict = state.get("terminated_times", {})
    all_ids = {j["id"] for j in jobs}

    for job in terminated:
        job_id = job["id"]
        name   = job.get("name", job_id[:8])

        if job_id not in term_times:
            term_times[job_id] = now.isoformat()
            info(f"记录终止时刻：{name} ({job_id[:8]}…) @ {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        else:
            recorded = datetime.fromisoformat(term_times[job_id])
            if recorded.tzinfo is None:
                recorded = recorded.replace(tzinfo=timezone.utc)
            age_h = (now - recorded).total_seconds() / 3600
            if age_h >= threshold_hours:
                _, code = macli("delete", job_id, "--yes")
                if code == 0:
                    info(f"已删除：{name} ({job_id[:8]}…)，已终止 {age_h:.1f}h")
                    del term_times[job_id]
                else:
                    error(f"删除失败：{name} ({job_id[:8]}…)")
            else:
                remain = threshold_hours - age_h
                info(f"保留中：{name} 已终止 {age_h:.1f}h，距清理还有 {remain:.1f}h")

    # 清理已不在作业列表中的终止记录（作业被手动删除）
    stale = [k for k in list(term_times) if k not in all_ids]
    for k in stale:
        del term_times[k]
    if stale:
        info(f"清理过期终止记录 {len(stale)} 条（作业已不存在）")

    state["terminated_times"] = term_times
    state["last_check"]       = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    save_state(state)
    maybe_send_disk_alert(disk_state)
    info("本次检查完成")


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="macli watch 检查脚本（可独立运行，也由 macli watch 调度）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python scripts/check_jobs.py                    # 默认阈值 72h
  python scripts/check_jobs.py --threshold-hours 48
""",
    )
    p.add_argument(
        "--threshold-hours", type=int, default=None,
        metavar="N",
        help=f"Terminated 作业终止多少小时后删除（默认读取 macli watch 配置，回退 {DEFAULT_THRESHOLD_H}）",
    )
    args = p.parse_args()

    threshold = args.threshold_hours
    if threshold is None:
        try:
            cfg_file = CONFIG_DIR / "session.json"
            if cfg_file.exists():
                threshold = json.loads(cfg_file.read_text(encoding="utf-8")).get(
                    "watch", {}
                ).get("threshold_hours", DEFAULT_THRESHOLD_H)
            else:
                threshold = DEFAULT_THRESHOLD_H
        except Exception:
            threshold = DEFAULT_THRESHOLD_H

    check(threshold_hours=threshold)
