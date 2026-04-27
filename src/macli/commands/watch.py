"""macli watch -- 定时检查任务（daemon / launchd / cron）"""
import os, sys, json, time, signal
from pathlib import Path

from macli.constants import _IS_LINUX, console
from macli.config import load_watch_cfg, save_watch_cfg
from macli.log import cprint, dprint
from macli.platform_daemon import DaemonManager, _run_check_once


# ── 常量 ────────────────────────────────────────────────────────
_WATCH_KEY         = "watch"
_WATCH_PLIST_LABEL = "com.macli.watch"
_WATCH_PLIST_PATH  = Path.home() / "Library" / "LaunchAgents" / f"{_WATCH_PLIST_LABEL}.plist"
_WATCH_STATE_FILE  = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "watch_state.json"
_WATCH_PID_FILE    = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "watch.pid"
_WATCH_LOG_FILE    = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "watch.log"
_WATCH_CRON_MARKER = "# macli-watch"

# ── DaemonManager 实例 ──────────────────────────────────────────
_watch_daemon = DaemonManager(
    name="watch",
    plist_label=_WATCH_PLIST_LABEL,
    plist_path=_WATCH_PLIST_PATH,
    pid_file=_WATCH_PID_FILE,
    log_file=_WATCH_LOG_FILE,
    cron_marker=_WATCH_CRON_MARKER,
)


# ── 命令入口 ────────────────────────────────────────────────────
def cmd_watch(args):
    action = getattr(args, "watch_action", "status")
    if action == "enable":
        _watch_enable(args)
    elif action == "disable":
        _watch_disable()
    elif action == "run":
        _watch_run(args)
    else:
        _watch_status(args)


# ── status ──────────────────────────────────────────────────────
def _watch_status(args=None):
    cfg    = load_watch_cfg()

    # 内联配置更新：传了参数时更新 cfg 并重启 daemon
    changed = False
    if args is not None:
        if getattr(args, "interval", None) is not None:
            cfg["interval_h"] = args.interval
            changed = True
        if getattr(args, "threshold_hours", None) is not None and args.threshold_hours != cfg.get("threshold_hours"):
            cfg["threshold_hours"] = args.threshold_hours
            changed = True
    if changed and cfg.get("enabled"):
        save_watch_cfg(cfg)
        cprint("[green]✓ 配置已更新，重启 watch...[/green]")
        if _IS_LINUX:
            try:
                cmd = [sys.executable, "-m", "macli", "watch", "run",
                       "--script", cfg.get("script_path", ""),
                       "--threshold-hours", str(cfg.get("threshold_hours", 72)),
                       "--interval", str(cfg["interval_h"])]
                _watch_daemon.linux_start(cmd)
            except RuntimeError as e:
                cprint(f"[red]{e}[/red]")
        # macOS 也重新加载
        elif _WATCH_PLIST_PATH.exists():
            _watch_daemon.launchctl("unload")
            program_args = [
                sys.executable, cfg.get("script_path", ""),
                "--threshold-hours", str(cfg.get("threshold_hours", 72)),
            ]
            _WATCH_PLIST_PATH.write_text(
                _watch_daemon.plist_xml(
                    program_args,
                    interval_secs=int(cfg["interval_h"] * 3600)),
                encoding="utf-8")
            _watch_daemon.launchctl("load")
    elif changed:
        save_watch_cfg(cfg)
        cprint("[green]✓ 配置已更新[/green]")

    if _IS_LINUX:
        running = _watch_daemon.linux_is_running()
        if cfg.get("enabled") and running:
            cprint("[green]watch：[bold]已启用（后台进程运行中）[/bold][/green]")
        elif cfg.get("enabled") and not running:
            cprint("[yellow]watch：已配置但进程未运行（执行 macli watch enable 重新启动）[/yellow]")
        else:
            cprint("[dim]watch：未启用[/dim]")
    else:
        loaded = _watch_daemon.launchctl_is_loaded()
        if cfg.get("enabled") and loaded:
            cprint("[green]watch：[bold]已启用（launchd 运行中）[/bold][/green]")
        elif cfg.get("enabled") and not loaded:
            cprint("[yellow]watch：已配置但 launchd 未加载（建议重新 enable）[/yellow]")
        else:
            cprint("[dim]watch：未启用[/dim]")

    if cfg:
        cprint(f"  检查脚本  : [dim]{cfg.get('script_path', '—')}[/dim]")
        cprint(f"  检查间隔  : {cfg.get('interval_h', '—')}h")
        cprint(f"  终止阈值  : {cfg.get('threshold_hours', 72)}h")
        cprint(f"  日志文件  : [dim]{cfg.get('log_path', '—')}[/dim]")

    if _WATCH_STATE_FILE.exists():
        try:
            state = json.loads(_WATCH_STATE_FILE.read_text(encoding="utf-8"))
            last  = state.get("last_check")
            terms = state.get("terminated_times", {})
            if last:
                cprint(f"  上次检查  : [dim]{last}[/dim]")
            if terms:
                cprint(f"  追踪终止作业: {len(terms)} 个")
                for jid, ts in list(terms.items())[:5]:
                    cprint(f"    [dim]{jid[:16]}… → {ts}[/dim]")
        except Exception:
            pass


# ── enable ──────────────────────────────────────────────────────
def _watch_enable(args):
    script_arg      = getattr(args, "script",          None)
    interval_h      = getattr(args, "interval",        None) or 1.0
    threshold_hours = getattr(args, "threshold_hours", 72)

    # 找脚本路径：参数 > 已保存配置 > 包内默认路径
    _bundled = Path(__file__).resolve().parents[2] / "scripts" / "check_jobs.py"
    if script_arg:
        script_path = Path(script_arg).expanduser().resolve()
    else:
        stored = load_watch_cfg().get("script_path", "")
        if stored and Path(stored).exists():
            script_path = Path(stored)
        elif _bundled.exists():
            script_path = _bundled
        else:
            cprint("[red]请用 --script 指定 check_jobs.py 的路径[/red]")
            cprint("[dim]示例：macli watch enable --script /path/to/scripts/check_jobs.py[/dim]")
            sys.exit(1)

    if not script_path.exists():
        cprint(f"[red]脚本不存在：{script_path}[/red]")
        sys.exit(1)

    log_path   = str(Path(os.environ.get("XDG_CONFIG_HOME",
                                          Path.home() / ".config")) / "macli" / "watch.log")
    interval_s = int(interval_h * 3600)

    cfg = {
        "enabled":         True,
        "interval_h":      interval_h,
        "script_path":     str(script_path),
        "threshold_hours": threshold_hours,
        "log_path":        log_path,
    }

    if _IS_LINUX:
        # 清理旧 cron 条目（从 cron 迁移到 daemon）
        if _watch_daemon.cron_is_active():
            _watch_daemon.cron_remove()
            cprint("[dim]已清理旧 cron 条目[/dim]")
        if _watch_daemon.linux_is_running():
            cprint("[dim]watch 已在运行，重新启动...[/dim]")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            cmd = [sys.executable, "-m", "macli", "watch", "run",
                   "--script", str(script_path),
                   "--threshold-hours", str(threshold_hours),
                   "--interval", str(interval_h)]
            _watch_daemon.linux_start(cmd)
        except RuntimeError as e:
            cprint(f"[red]{e}[/red]")
            sys.exit(1)
        save_watch_cfg(cfg)
        cprint(f"[green]✓ watch 已启用，每 {interval_h}h 执行一次（daemon）[/green]")
        cprint(f"  脚本：{script_path}")
        cprint(f"  日志：{log_path}")
    else:
        if _watch_daemon.launchctl_is_loaded():
            cprint("[dim]watch 已在运行，重新加载...[/dim]")
        # 写 plist
        _WATCH_PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _watch_daemon.launchctl("unload")   # 先卸载（忽略失败）
        program_args = [
            sys.executable, str(script_path),
            "--threshold-hours", str(threshold_hours),
        ]
        _WATCH_PLIST_PATH.write_text(
            _watch_daemon.plist_xml(program_args, interval_secs=interval_s),
            encoding="utf-8",
        )

        if _watch_daemon.launchctl("load"):
            save_watch_cfg(cfg)
            cprint(f"[green]✓ watch 已启用，每 {interval_h}h 执行一次[/green]")
            cprint(f"  脚本：{script_path}")
            cprint(f"  日志：{log_path}")
            cprint(f"  plist：{_WATCH_PLIST_PATH}")
        else:
            cprint("[red]launchctl load 失败[/red]")
            cprint(f"  plist：{_WATCH_PLIST_PATH}")
            sys.exit(1)


# ── disable ─────────────────────────────────────────────────────
def _watch_disable():
    if _IS_LINUX:
        _watch_daemon.linux_stop()
        # 也清理可能残留的旧 cron 条目
        if _watch_daemon.cron_is_active():
            _watch_daemon.cron_remove()
        cprint("[green]✓ watch 已停用，后台进程已终止[/green]")
    else:
        _watch_daemon.launchctl("unload")
        if _WATCH_PLIST_PATH.exists():
            _WATCH_PLIST_PATH.unlink()
        cprint("[green]✓ watch 已停用，launchd 任务已卸载[/green]")

    cfg = load_watch_cfg()
    cfg["enabled"] = False
    save_watch_cfg(cfg)


# ── run (单次执行 or daemon 循环) ───────────────────────────────
def _watch_run(args):
    """执行检查脚本。有 --interval 时作为 daemon 循环运行。"""
    cfg             = load_watch_cfg()
    script_arg      = getattr(args, "script",          None)
    threshold_hours = getattr(args, "threshold_hours",  None)
    interval_h      = getattr(args, "interval",         None)

    script_path = Path(script_arg).expanduser() if script_arg else Path(cfg.get("script_path", ""))
    if not script_path.exists():
        cprint("[red]未找到检查脚本，请先 macli watch enable --script PATH 或用 --script 指定[/red]")
        sys.exit(1)

    if threshold_hours is None:
        threshold_hours = cfg.get("threshold_hours", 72)

    if interval_h is None:
        # 单次执行模式（向后兼容）
        cprint(f"[cyan]立即运行：{script_path}[/cyan]")
        sys.exit(_run_check_once(script_path, threshold_hours))

    # daemon 循环模式
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    interval_s = int(interval_h * 3600)
    cprint(f"[cyan]watch daemon 启动，每 {interval_h}h 检查一次[/cyan]")
    while True:
        _run_check_once(script_path, threshold_hours)
        time.sleep(interval_s)
