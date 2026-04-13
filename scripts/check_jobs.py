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
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 路径配置 ──────────────────────────────────────────────────
SCRIPTS_DIR    = Path(__file__).parent.resolve()
STARTUP_SCRIPT = SCRIPTS_DIR / "startup.sh"
CONFIG_DIR     = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli"
STATE_FILE     = CONFIG_DIR / "watch_state.json"
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

# ── 检查逻辑 ──────────────────────────────────────────────────
def check(threshold_hours: int = DEFAULT_THRESHOLD_H, region: str = ""):
    now   = datetime.now(timezone.utc)

    if region:
        info(f"切换区域：{region}")
        out, code = macli("region", "select", "--name", region)
        if code != 0:
            error(f"区域切换失败：{region}")
            return

    state = load_state()
    jobs  = get_jobs()

    if not jobs and not state.get("terminated_times"):
        warn("无法获取作业列表，跳过本次检查")
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
    p.add_argument(
        "--region", default=None,
        metavar="REGION",
        help="每次检查前切换到指定区域（如 cn-north-9）",
    )
    args = p.parse_args()

    # 从 session.json watch 配置读取默认值
    watch_cfg = {}
    try:
        cfg_file = CONFIG_DIR / "session.json"
        if cfg_file.exists():
            watch_cfg = json.loads(cfg_file.read_text(encoding="utf-8")).get("watch", {})
    except Exception:
        pass

    threshold = args.threshold_hours
    if threshold is None:
        threshold = watch_cfg.get("threshold_hours", DEFAULT_THRESHOLD_H)

    region = args.region
    if region is None:
        region = watch_cfg.get("region", "")

    check(threshold_hours=threshold, region=region)
