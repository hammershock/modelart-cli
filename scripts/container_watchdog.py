#!/usr/bin/env python3
"""
container_watchdog.py — ModelArts debug 容器资源守护进程

行为与内核 OOM killer 一致：在资源耗尽前主动干掉最大的消费进程，
而不是让整个容器被 K8s 驱逐。

监控项：
  - /cache/ 磁盘用量   ≥ CACHE_KILL_PCT(90%)  → SIGTERM 开销最大的进程
  - 内存 cgroup 用量   ≥ MEM_KILL_PCT(85%)    → SIGTERM 开销最大的进程
  - 10 秒后进程仍存活  →  SIGKILL

告警：写入 /temp/WATCHDOG_ALERT.txt（NFS 持久挂载，容器重启后仍可查看）

使用方式（上传到 /temp/ 一次，之后所有 job 共用）：
  nohup python3 /temp/container_watchdog.py >> /tmp/watchdog.log 2>&1 &

推荐启动命令（完整版，见文末注释）：
  ulimit -c 0
  echo "ulimit -c 0" >> ~/.bashrc
  mkdir -p ~/workspace/
  nohup python3 /temp/container_watchdog.py >> /tmp/watchdog.log 2>&1 &
  sleep 2000000000s
"""

import os
import sys
import time
import signal
import logging
import subprocess
from pathlib import Path
from collections import Counter

# ── 配置 ──────────────────────────────────────────────────────────────────────
CACHE_KILL_PCT   = int(os.environ.get("WATCHDOG_CACHE_PCT", 90))   # /cache/ 触发阈值
MEM_KILL_PCT     = int(os.environ.get("WATCHDOG_MEM_PCT",   85))   # 内存 cgroup 触发阈值
KILL_GRACE_SEC   = int(os.environ.get("WATCHDOG_GRACE",     10))   # SIGTERM→SIGKILL 宽限秒数
INTERVAL_SEC     = int(os.environ.get("WATCHDOG_INTERVAL",  20))   # 检查间隔秒数
ALERT_FILE       = os.environ.get("WATCHDOG_ALERT",  "/temp/WATCHDOG_ALERT.txt")

# 永远不杀的进程名前缀（平台基础进程）
PROTECTED_COMMS = {
    "bash", "sh", "zsh", "sshd", "init", "systemd",
    "python3", "watchdog",           # 守护进程自身
    "sleep",                          # 启动命令中的占位 sleep
    "ma-training-toolki", "bootstrap", "obs",  # ModelArts sidecar（comm 截断到 15 字符）
}

# ── 日志 ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="[watchdog %(asctime)s] %(levelname)s  %(message)s",
    datefmt="%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stderr,
)
log = logging.getLogger("watchdog")

# ── 资源检测 ──────────────────────────────────────────────────────────────────

def cache_pct() -> int:
    """返回 /cache/ 磁盘使用百分比，读取失败返回 0。"""
    try:
        st = os.statvfs("/cache/")
        total = st.f_blocks * st.f_frsize
        used  = (st.f_blocks - st.f_bfree) * st.f_frsize
        return int(used * 100 / total) if total else 0
    except OSError:
        return 0


def mem_pct() -> int:
    """返回容器内存 cgroup 使用百分比，无限制或读取失败返回 0。"""
    try:
        usage = int(Path("/sys/fs/cgroup/memory/memory.usage_in_bytes").read_text())
        limit = int(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").read_text())
        if limit > 9 * 10**15:   # 实质上无限制（9 PB）
            return 0
        return int(usage * 100 / limit)
    except Exception:
        return 0


def comm_of(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/comm").read_text().strip()
    except OSError:
        return ""


def is_protected(pid: int) -> bool:
    c = comm_of(pid)
    return any(c.startswith(p) for p in PROTECTED_COMMS)

# ── 寻找目标进程 ──────────────────────────────────────────────────────────────

def top_cache_pid() -> int | None:
    """
    返回在 /cache/ 下打开文件数最多的非保护进程 PID。
    用 lsof 查找；若 lsof 不可用则退回到遍历 /proc/*/fd。
    """
    try:
        result = subprocess.run(
            ["lsof", "-t", "+D", "/cache/"],
            capture_output=True, text=True, timeout=15,
        )
        pids = [int(p) for p in result.stdout.split() if p.strip().isdigit()]
    except Exception:
        # fallback：遍历 /proc
        pids = []
        for p in Path("/proc").iterdir():
            if not p.name.isdigit():
                continue
            try:
                for fd in (p / "fd").iterdir():
                    target = os.readlink(str(fd))
                    if target.startswith("/cache/"):
                        pids.append(int(p.name))
                        break
            except (OSError, PermissionError):
                continue

    candidates = [pid for pid in set(pids) if not is_protected(pid)]
    if not candidates:
        return None
    # 出现次数最多 = 打开文件数最多 = 最可能是当前写入者
    pid, _ = Counter(pids).most_common(1)[0]
    return pid if not is_protected(pid) else None


def top_mem_pid() -> int | None:
    """
    返回 RSS 最大的非保护进程 PID（预防性 OOM killer）。
    """
    best_pid, best_rss = None, 0
    for p in Path("/proc").iterdir():
        if not p.name.isdigit():
            continue
        pid = int(p.name)
        if is_protected(pid):
            continue
        try:
            stat = (p / "stat").read_text().split()
            rss  = int(stat[23]) * 4096          # pages → bytes
            if rss > best_rss:
                best_rss, best_pid = rss, pid
        except Exception:
            continue
    return best_pid

# ── 动作 ──────────────────────────────────────────────────────────────────────

def kill_pid(pid: int, reason: str) -> None:
    """SIGTERM，等 KILL_GRACE_SEC 秒，仍存活则 SIGKILL。"""
    comm = comm_of(pid)
    log.warning(f"KILLING pid={pid} ({comm})  reason: {reason}")
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(KILL_GRACE_SEC)
    try:
        os.kill(pid, signal.SIGKILL)
        log.warning(f"SIGKILL sent to pid={pid} ({comm})  (did not exit after SIGTERM)")
    except ProcessLookupError:
        pass   # SIGTERM 已生效
    write_alert(f"Killed pid={pid} ({comm})\nReason: {reason}")


def write_alert(body: str) -> None:
    try:
        with open(ALERT_FILE, "w") as f:
            f.write("=" * 42 + "\n")
            f.write("  WATCHDOG ALERT\n")
            f.write("=" * 42 + "\n")
            f.write(f"Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Job:    {os.environ.get('MA_VJ_NAME', 'unknown')}\n")
            f.write(f"Node:   {os.environ.get('MA_CURRENT_HOST_IP', 'unknown')}\n")
            f.write(f"\n{body}\n")
            f.write("\n查看 /cache/ 使用情况：\n")
            f.write("  du -sh /cache/*/\n")
            f.write("  find /cache/ -size +1G -ls\n")
    except OSError:
        pass

# ── 主循环 ────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info(
        f"Started  cache_kill≥{CACHE_KILL_PCT}%  "
        f"mem_kill≥{MEM_KILL_PCT}%  "
        f"interval={INTERVAL_SEC}s  "
        f"grace={KILL_GRACE_SEC}s"
    )

    while True:
        # ── /cache/ 检查 ──────────────────────────────────────────────────────
        cpct = cache_pct()
        if cpct >= CACHE_KILL_PCT:
            log.warning(f"/cache/ at {cpct}%  (threshold {CACHE_KILL_PCT}%)")
            pid = top_cache_pid()
            if pid:
                kill_pid(pid, f"/cache/ disk usage {cpct}%")
            else:
                msg = f"/cache/ at {cpct}% but no killable process found — manual cleanup needed"
                log.error(msg)
                write_alert(msg)

        # ── 内存检查 ──────────────────────────────────────────────────────────
        mpct = mem_pct()
        if mpct >= MEM_KILL_PCT:
            log.warning(f"memory cgroup at {mpct}%  (threshold {MEM_KILL_PCT}%)")
            pid = top_mem_pid()
            if pid:
                kill_pid(pid, f"memory cgroup usage {mpct}%")
            else:
                log.warning("memory cgroup high but no killable process found")

        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
