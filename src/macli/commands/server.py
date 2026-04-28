"""macli server -- FastAPI 本地状态服务"""
import os, sys, json, time, re, subprocess as _subprocess
from pathlib import Path
from datetime import datetime

from macli.constants import (_CST, _IS_LINUX, SessionExpiredError, console,
                             _ME_PROBE_REGIONS)
from macli.config import (load_session, save_session, load_server_cfg, save_server_cfg,
                          load_watch_cfg, load_auto_login_cfg, load_identityfiles,
                          _AUTOLOGIN_KEY)
from macli.log import cprint, dprint
from macli.platform_daemon import DaemonManager, _run_check_once


# ── 常量 ────────────────────────────────────────────────────────
_SERVER_KEY         = "server"
_SERVER_PLIST_LABEL = "com.macli.server"
_SERVER_PLIST_PATH  = Path.home() / "Library" / "LaunchAgents" / f"{_SERVER_PLIST_LABEL}.plist"
_SERVER_LOG_FILE    = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "server.log"
_SERVER_PID_FILE    = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "server.pid"
_MACLI_LOG_FILE     = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "macli.log"

# watch 常量（health endpoint 需要）
_WATCH_KEY         = "watch"
_WATCH_STATE_FILE  = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "watch_state.json"
_DISK_STATE_FILE   = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "disk_state.json"

# ── DaemonManager 实例 ──────────────────────────────────────────
_server_daemon = DaemonManager(
    name="server",
    plist_label=_SERVER_PLIST_LABEL,
    plist_path=_SERVER_PLIST_PATH,
    pid_file=_SERVER_PID_FILE,
    log_file=_SERVER_LOG_FILE,
)


# ── 命令入口 ────────────────────────────────────────────────────
def cmd_server(args):
    action = getattr(args, "server_action", None) or "status"
    if action == "enable":
        _server_enable(args)
    elif action == "disable":
        _server_disable()
    elif action == "run":
        _server_run(args)
    else:
        _server_status()


# ── status ──────────────────────────────────────────────────────
def _server_status():
    cfg    = load_server_cfg()
    port   = cfg.get("port", 8086)
    if _IS_LINUX:
        running = _server_daemon.linux_is_running()
        if cfg.get("enabled") and running:
            cprint("[green]server：已启用（后台进程运行中）[/green]")
        elif cfg.get("enabled"):
            cprint("[yellow]server：已配置但进程未运行（执行 macli server enable 重新启动）[/yellow]")
        else:
            cprint("[dim]server：未启用[/dim]")
    else:
        loaded = _server_daemon.launchctl_is_loaded()
        if cfg.get("enabled") and loaded:
            cprint("[green]server：已启用（launchd 运行中）[/green]")
        elif cfg.get("enabled"):
            cprint("[yellow]server：已配置但 launchd 未运行（执行 macli server enable 重新加载）[/yellow]")
        else:
            cprint("[dim]server：未启用[/dim]")
    if cfg:
        cprint(f"  端口        : {port}")
        cprint(f"  /gpu        : http://localhost:{port}/gpu")
        cprint(f"  /ports      : http://localhost:{port}/ports")
        cprint(f"  /log        : http://localhost:{port}/log")
        cprint(f"  /watch-log  : http://localhost:{port}/watch-log")
        cprint(f"  /server-log : http://localhost:{port}/server-log")
        cprint(f"  /health     : http://localhost:{port}/health")
        cprint(f"  日志文件    : [dim]{_SERVER_LOG_FILE}[/dim]")


# ── enable ──────────────────────────────────────────────────────
def _server_enable(args):
    port = getattr(args, "port", None) or load_server_cfg().get("port", 8086)
    cfg = load_server_cfg()
    cfg.update({"enabled": True, "port": port})
    save_server_cfg(cfg)
    if _IS_LINUX:
        if _server_daemon.linux_is_running():
            cprint("[dim]server 已在运行，重新启动...[/dim]")
        try:
            cmd = [sys.executable, "-m", "macli", "server", "run", "--port", str(port)]
            _server_daemon.linux_start(cmd)
        except RuntimeError as e:
            cprint(f"[red]{e}[/red]")
            sys.exit(1)
        cprint(f"[green]✓ server 已启用  http://localhost:{port}/gpu[/green]")
        cprint(f"  日志：{_SERVER_LOG_FILE}")
    else:
        if _server_daemon.launchctl_is_loaded():
            cprint("[dim]server 已在运行，重新加载...[/dim]")
        _SERVER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SERVER_PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _server_daemon.launchctl("unload")
        program_args = [
            sys.executable, "-m", "macli",
            "server", "run",
            "--port", str(port),
        ]
        _SERVER_PLIST_PATH.write_text(
            _server_daemon.plist_xml(program_args, keep_alive=True, run_at_load=True),
            encoding="utf-8",
        )
        ok = _server_daemon.launchctl("load")
        if ok:
            cprint(f"[green]✓ server 已启用  http://localhost:{port}/gpu[/green]")
        else:
            cprint(f"[yellow]⚠ 配置已写入，launchctl load 返回非零（可能已在运行）[/yellow]")
            cprint(f"  http://localhost:{port}/gpu")


# ── disable ─────────────────────────────────────────────────────
def _server_disable():
    if _IS_LINUX:
        _server_daemon.linux_stop()
    else:
        _server_daemon.launchctl("unload")
        if _SERVER_PLIST_PATH.exists():
            _SERVER_PLIST_PATH.unlink()
    cfg = load_server_cfg()
    cfg["enabled"] = False
    save_server_cfg(cfg)
    cprint("[green]✓ server 已停用[/green]")


# ── run (FastAPI server, 阻塞) ──────────────────────────────────
def _server_run(args):
    """在当前进程内启动 FastAPI server（阻塞）。由 launchd 或手动调用。"""
    import threading as _threading
    from io import StringIO as _StringIO

    port = getattr(args, "port", None) or load_server_cfg().get("port", 8086)

    try:
        import fastapi as _fastapi
        import uvicorn as _uvicorn
    except ImportError:
        _subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "fastapi", "uvicorn[standard]"]
        )
        import fastapi as _fastapi
        import uvicorn as _uvicorn

    from fastapi import FastAPI as _FastAPI, Request as _Request
    from fastapi.responses import PlainTextResponse as _Plain, JSONResponse as _JSON
    from rich.console import Console as _RConsole
    from rich.table import Table as _RTable
    from rich.text import Text as _RText

    # 延迟导入：跨模块依赖
    from macli.auth import _do_auto_login, _autologin_record_outcome
    from macli.session import ConsoleSession, API
    from macli.helpers import job_to_dict, _fetch_all_jobs

    # watch daemon 实例（health endpoint 中判断 watch 状态）
    from macli.commands.watch import _watch_daemon

    _RATE_LIMIT = 10.0
    _cache_lock = _threading.Lock()
    _cache      = {"last_run": 0.0, "last_run_ts": 0.0, "ansi": "", "plain": "", "jobs": []}

    # ── 通用缓存子进程调用 ──────────────────────────────────
    class _CachedCall:
        """对 macli 子进程调用结果按 TTL 缓存，线程安全。"""
        def __init__(self, ttl: float):
            self.ttl      = ttl
            self.last_run = 0.0
            self.result   = None   # None = 尚未采集
            self.lock     = _threading.Lock()

        def get(self, fetch_fn):
            """返回缓存值（未过期）或执行 fetch_fn() 刷新后返回。"""
            with self.lock:
                age = time.monotonic() - self.last_run
                if self.result is None or age >= self.ttl:
                    self.result   = fetch_fn()
                    self.last_run = time.monotonic()
                return self.result, round(time.monotonic() - self.last_run, 1)

    _ports_cache  = _CachedCall(ttl=30.0)
    _health_cache = _CachedCall(ttl=3.0)
    _jobs_cache   = _CachedCall(ttl=30.0)
    _srv_log: list = []
    _srv_log_lock  = _threading.Lock()

    # ── 浏览器检测 ─────────────────────────────────────────
    def _is_browser(req: _Request) -> bool:
        ua     = (req.headers.get("user-agent") or "").lower()
        accept = (req.headers.get("accept")     or "").lower()
        if any(x in ua for x in ("curl/", "wget/", "httpie/", "python-requests",
                                  "go-http-client", "postmanruntime/")):
            return False
        if any(x in ua for x in ("mozilla/", "chrome/", "safari/", "firefox/", "edg/")):
            return True
        return "text/html" in accept and "text/plain" not in accept

    # ── 请求日志 ────────────────────────────────────────────
    def _log_req(method: str, path: str, status: int, ms: float, ip: str = "-"):
        ts   = datetime.now(_CST).strftime("%Y-%m-%dT%H:%M:%S+08")
        line = f"{ts} {ip} {method} {path} {status} {ms:.0f}ms"
        with _srv_log_lock:
            _srv_log.append(line)
            if len(_srv_log) > 10000:
                del _srv_log[:-10000]
        try:
            _SERVER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(_SERVER_LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass

    # ── 渲染 macli usage --probe --json → 表格 ─────────────
    def _fmt_pct(v):
        return f"{round((v or 0) * 100)}%" if v is not None else "—"

    def _fmt_mem(mb):
        if mb is None:
            return "—"
        mb = mb or 0
        if mb >= 1024:
            return f"{mb / 1024:.1f}GB"
        if mb >= 1:
            return f"{round(mb)}MB"
        return f"{round(mb * 1024)}KB"

    def _fmt_created(ts):
        if not ts:
            return "—"
        try:
            return datetime.fromtimestamp(int(ts) / 1000, tz=_CST).strftime("%y-%m-%d")
        except Exception:
            return "—"

    def _fmt_dur(ms):
        if not ms:
            return "—"
        try:
            h, rem = divmod(int(ms) // 1000, 3600)
            m, s   = divmod(rem, 60)
            return f"{h}:{m:02d}:{s:02d}"
        except Exception:
            return "—"

    def _dev_cell(d, use_ansi):
        util       = d.get("util")
        vram_used  = d.get("vram_used_mb")
        vram_total = d.get("vram_total_mb")
        text = (f"gpu{d.get('index', '?')} {_fmt_pct(util)}"
                f" {_fmt_mem(vram_used)}/{_fmt_mem(vram_total)}")
        if not use_ansi:
            return text
        u        = (util or 0) * 100
        vram_pct = (vram_used or 0) / (vram_total or 1) * 100
        if u == 0 and vram_pct < 3:
            color = "green"
        elif u > 60 or vram_pct > 60:
            color = "red"
        else:
            color = "yellow"
        return _RText(text, style=color)

    def _load_disk_state() -> dict:
        try:
            if _DISK_STATE_FILE.exists():
                return json.loads(_DISK_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
        return {}

    def _disk_job_index(disk: dict) -> dict:
        idx = {}
        for host in ((disk or {}).get("hosts") or {}).values():
            df = host.get("df") or {}
            used = int(df.get("used_bytes") or 0)
            total = int(df.get("total_bytes") or 0)
            evict_limit = int(total * 0.9)
            alloc_pct = used / evict_limit * 100 if evict_limit > 0 else None
            margin_bytes = evict_limit - used if evict_limit > 0 else None
            for job in host.get("jobs", []):
                job_id = job.get("id")
                if not job_id:
                    continue
                cache = job.get("cache_bytes")
                share_pct = (int(cache) / used * 100
                             if cache is not None and used > 0 else None)
                idx[job_id] = {
                    "alloc_pct": alloc_pct,
                    "share_pct": share_pct,
                    "margin_bytes": margin_bytes,
                }
        return idx

    def _disk_pct_text(value) -> str:
        return f"{value:.2f}%" if value is not None else "—"

    def _disk_style(value, thresholds: tuple) -> str:
        if value is None:
            return "dim"
        yellow, orange, red = thresholds
        if value >= red:
            return "red"
        if value >= orange:
            return "orange1"
        if value >= yellow:
            return "yellow"
        return "green"

    def _disk_level(value, thresholds: tuple) -> str:
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

    def _disk_cell(value, thresholds: tuple, use_ansi: bool, pct_symbol_only: bool = False):
        text = _disk_pct_text(value)
        if not use_ansi:
            return text
        if pct_symbol_only and value is not None:
            cell = _RText(f"{value:.2f}", style="grey50")
            cell.append("%", style=_disk_style(value, thresholds))
            return cell
        return _RText(text, style=_disk_style(value, thresholds))

    def _disk_bytes_text(value) -> str:
        if value is None:
            return "—"
        sign = "-" if value < 0 else ""
        n = abs(float(value))
        for factor, suffix in ((1024 ** 4, "TB"), (1024 ** 3, "GB"),
                               (1024 ** 2, "MB"), (1024, "KB")):
            if n >= factor:
                v = n / factor
                digits = 1 if v < 10 else 0
                text = f"{v:.{digits}f}".rstrip("0").rstrip(".")
                return f"{sign}{text}{suffix}"
        return f"{sign}{int(n)}B"

    def _disk_bytes_cell(value, alloc_value, use_ansi: bool):
        text = _disk_bytes_text(value)
        if not use_ansi:
            return text
        return _RText(text, style=_disk_style(alloc_value, (50, 70, 85)))

    def _render_jobs(jobs: list, use_ansi: bool, disk: dict = None) -> str:
        if not jobs:
            return "No running jobs.\n"
        disk = disk or {}
        disk_idx = _disk_job_index(disk)
        show_disk = bool(disk)

        buf = _StringIO()
        con = _RConsole(file=buf, force_terminal=use_ansi, force_jupyter=False,
                        highlight=False, markup=False, width=152,
                        color_system="truecolor" if use_ansi else None)
        tbl = _RTable(show_header=True,
                      header_style="bold cyan" if use_ansi else "",
                      show_lines=False, pad_edge=False)
        for col, kw in [("",       dict(width=2,       no_wrap=True)),
                        ("job",    dict(min_width=8,   no_wrap=True)),
                        ("ssh",    dict(width=7,        no_wrap=True)),
                        ("cpu%",   dict(width=5,        no_wrap=True)),
                        ("mem",    dict(width=8,        no_wrap=True)),
                        ("created",dict(width=10,       no_wrap=True)),
                        ("dur",    dict(width=10,       no_wrap=True)),
                        ("devices",dict(min_width=32,   no_wrap=False))]:
            tbl.add_column(col, **kw)
        if show_disk:
            tbl.add_column("alloc%", width=8, no_wrap=True)
            tbl.add_column("margin", width=9, no_wrap=True)
            tbl.add_column("share%", width=8, no_wrap=True)
        for r in jobs:
            devs      = r.get("gpu_devices", [])
            job_id    = r.get("job_id") or "?"
            job_short = job_id[:8]
            quota_class = r.get("quota_class", "unknown")
            flag      = {
                "guaranteed": "🏠",
                "elastic": "🔴",
                "unknown": "?",
                "inactive": "-",
            }.get(quota_class, "?")
            ssh       = r.get("ssh_port") or "—"
            cpu       = _fmt_pct(r.get("cpu"))
            mem       = _fmt_mem(r.get("mem"))
            created   = _fmt_created(r.get("create_time"))
            dur       = _fmt_dur(r.get("duration_ms"))
            disk_info = disk_idx.get(job_id, {})
            alloc_value = disk_info.get("alloc_pct")
            share_value = disk_info.get("share_pct")
            margin_value = disk_info.get("margin_bytes")
            alloc = _disk_cell(alloc_value, (50, 70, 85), use_ansi)
            margin = _disk_bytes_cell(margin_value, alloc_value, use_ansi)
            share = _disk_cell(share_value, (10, 30, 50),
                               use_ansi, pct_symbol_only=True)
            if use_ansi:
                alloc_level = _disk_level(alloc_value, (50, 70, 85))
                share_level = _disk_level(share_value, (10, 30, 50))
                if alloc_level == "red" and share_level == "red":
                    flag = "🚨"
                    ssh = _RText(str(ssh), style="red")
                elif {alloc_level, share_level} == {"red", "orange"}:
                    ssh = _RText(str(ssh), style="orange1")
            row_base = [flag, job_short, ssh, cpu, mem, created, dur]
            if not devs:
                row = row_base + ["—"]
                if show_disk:
                    row += [alloc, margin, share]
                tbl.add_row(*row)
            else:
                for i, d in enumerate(devs):
                    cell = _dev_cell(d, use_ansi)
                    if i == 0:
                        row = row_base + [cell]
                        if show_disk:
                            row += [alloc, margin, share]
                        tbl.add_row(*row)
                    else:
                        row = ["", "", "", "", "", "", "", cell]
                        if show_disk:
                            row += ["", "", ""]
                        tbl.add_row(*row)
        con.print(tbl)
        body = buf.getvalue()
        if show_disk:
            note = ("Allocation >= 100% is sufficient to trigger eviction. "
                    "Eviction targets the JOB with the largest current share; "
                    "share is this JOB's cache usage divided by allocated space. "
                    "Other JOBs may exist and actual usage may be unknown, so share is only a decision aid.")
            if use_ansi:
                body += f"\033[38;5;244m{note}\033[0m\n"
            else:
                body += note + "\n"
        return body

    def _refresh():
        env = {**os.environ, "MACLI_NO_AUTOLOGIN": "1"}
        result = _subprocess.run(
            [sys.executable, "-m", "macli", "usage", "--probe", "--json"],
            capture_output=True, text=True, timeout=120, env=env,
        )
        # exit code 2 = session expired；在 server 进程内执行 autologin 后重试
        if result.returncode == 2:
            al_cfg = load_auto_login_cfg()
            if al_cfg.get("enabled"):
                dprint("[dim]server: session 过期，触发自动登录[/dim]")
                if _do_auto_login(al_cfg):
                    _autologin_record_outcome(True)
                    wcfg = load_watch_cfg()
                    if wcfg.get("enabled"):
                        sp = wcfg.get("script_path", "")
                        if sp and Path(sp).exists():
                            _run_check_once(Path(sp), wcfg.get("threshold_hours", 72))
                    result = _subprocess.run(
                        [sys.executable, "-m", "macli", "usage", "--probe", "--json"],
                        capture_output=True, text=True, timeout=120,
                    )
                else:
                    _autologin_record_outcome(False)

        jobs = None
        if result.returncode == 0:
            try:
                data = json.loads(result.stdout)
                jobs = data.get("jobs", [])
                disk = _load_disk_state()
                ansi  = _render_jobs(jobs, True, disk=disk)
                plain = _render_jobs(jobs, False, disk=disk)
            except (json.JSONDecodeError, KeyError) as e:
                ansi = plain = f"parse error: {e}\n{result.stdout[:300]}\n"
        else:
            err  = (result.stdout or result.stderr or "").strip()[:400]
            ansi = plain = f"Error (exit {result.returncode}):\n{err}\n"
        with _cache_lock:
            _cache["ansi"]     = ansi
            _cache["plain"]    = plain
            if jobs is not None:
                _cache["jobs"] = jobs
            _cache["last_run"]    = time.monotonic()
            _cache["last_run_ts"] = time.time()

    # ── tail 工具 ───────────────────────────────────────────
    def _tail(path: Path, n: int) -> str:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            return "\n".join(lines[-n:]) + "\n"
        except FileNotFoundError:
            return "(log file not found)\n"
        except OSError as e:
            return f"(error: {e})\n"

    # ── OTP slot (for autologin webhook) ─────────────────────
    import asyncio as _asyncio
    _otp_slot: dict = {"code": "", "expires": 0.0, "event": _asyncio.Event()}

    # ── App ─────────────────────────────────────────────────
    app = _FastAPI(title="macli server")

    @app.middleware("http")
    async def _access_log(req: _Request, call_next):
        t0 = time.monotonic()
        try:
            resp   = await call_next(req)
            status = resp.status_code
        except Exception:
            status = 500
            raise
        finally:
            ip = (req.headers.get("x-forwarded-for") or
                  req.headers.get("x-real-ip") or
                  (req.client.host if req.client else "-"))
            _log_req(req.method, req.url.path, status,
                     (time.monotonic() - t0) * 1000, ip)
        return resp

    @app.get("/gpu", response_class=_Plain)
    def get_gpu(req: _Request):
        with _cache_lock:
            age      = time.monotonic() - _cache["last_run"]
            has_data = bool(_cache["last_run"])
        if not has_data or age >= _RATE_LIMIT:
            _refresh()
            age = 0.0
        browser = _is_browser(req)
        with _cache_lock:
            body = _cache["plain" if browser else "ansi"]
        if age > 0:
            remain = round(_RATE_LIMIT - age, 1)
            body   = f"# [cached] last updated {round(age,1)}s ago (refresh in {remain}s)\n" + body
        return _Plain(body, headers={"X-Cache": "HIT" if age > 0 else "MISS",
                                     "X-Cache-Age": str(round(age, 1))})

    @app.get("/gpu.json")
    def get_gpu_json():
        with _cache_lock:
            age      = time.monotonic() - _cache["last_run"]
            has_data = bool(_cache["last_run"])
        if not has_data or age >= _RATE_LIMIT:
            _refresh()
            age = 0.0
        with _cache_lock:
            jobs = list(_cache["jobs"])
        out = []
        for r in jobs:
            devices = []
            for d in (r.get("gpu_devices") or []):
                util      = d.get("util")
                vram_used = d.get("vram_used_mb") or 0
                vram_tot  = d.get("vram_total_mb") or 1
                idle = ((util or 0) * 100 == 0 and vram_used / vram_tot * 100 < 3)
                devices.append({**d, "idle": idle})
            row = {**r, "gpu_devices": devices}
            if "quota_class" not in row:
                row.update({
                    "quota_class": "unknown",
                    "quota_labels": ["unknown", "preemptible"],
                    "quota_reason": "quota metadata unavailable",
                })
            if "preemptible" not in row:
                row["preemptible"] = row.get("quota_class") != "guaranteed"
            out.append(row)
        from fastapi.responses import JSONResponse as _JResp
        return _JResp(content=out, headers={"X-Cache-Age": str(round(age, 1))})

    @app.get("/log", response_class=_Plain)
    def get_macli_log():
        return _Plain(_tail(_MACLI_LOG_FILE, 1000))

    @app.get("/server-log", response_class=_Plain)
    def get_server_log():
        with _srv_log_lock:
            recent = list(_srv_log[-1000:])
        return _Plain("\n".join(recent) + "\n" if recent else "(no requests yet)\n")

    def _fetch_jobs_for_health() -> list:
        try:
            sess = ConsoleSession()
            if not sess.restore():
                return []
            api  = API(sess)
            return [job_to_dict(j) for j in _fetch_all_jobs(api)]
        except Exception:
            return []

    def _fetch_health():
        import datetime as _dt
        sess = load_session()
        ck   = sess.get("cookies", {})

        # ── login ─────────────────────────────────────────
        saved_at  = sess.get("saved_at", 0)
        age_h     = round((time.time() - saved_at) / 3600, 1) if saved_at else None
        login = {
            "logged_in":         bool(ck and sess.get("project_id")),
            "user":              ck.get("masked_user", ""),
            "domain":            ck.get("masked_domain", ""),
            "session_age_hours": age_h,
        }

        # ── server ────────────────────────────────────────
        srv = sess.get(_SERVER_KEY, {})
        server = {
            "enabled": srv.get("enabled", False),
            "running": _server_daemon.linux_is_running() if _IS_LINUX else _server_daemon.launchctl_is_loaded(),
            "port":    srv.get("port", 8086),
        }

        # ── watch ─────────────────────────────────────────
        wch = sess.get(_WATCH_KEY, {})
        last_check = None
        try:
            if _WATCH_STATE_FILE.exists():
                ws = json.loads(_WATCH_STATE_FILE.read_text(encoding="utf-8"))
                last_check = ws.get("last_check")
        except Exception:
            pass
        watch = {
            "enabled":         wch.get("enabled", False),
            "running":         _watch_daemon.cron_is_active() if _IS_LINUX else _watch_daemon.launchctl_is_loaded(),
            "interval_h":      wch.get("interval_h"),
            "threshold_hours": wch.get("threshold_hours"),
            "last_check":      last_check,
        }

        # ── autologin ─────────────────────────────────────
        al = sess.get(_AUTOLOGIN_KEY, {})
        autologin = {
            "enabled":              al.get("enabled", False),
            "otp_channel":          "webhook" if al.get("webhook_url") else ("ntfy" if al.get("ntfy_topic") else "none"),
            "webhook_url":          al.get("webhook_url", ""),
            "circuit_tripped":      al.get("circuit_tripped", False),
            "consecutive_failures": al.get("consecutive_failures", 0),
            "circuit_breaker":      al.get("circuit_breaker", 3),
            "last_autologin_ts":    al.get("last_autologin_ts", 0),
        }

        # ── exec / identityfiles ──────────────────────────
        idf_map, idf_default = load_identityfiles()
        exec_info = {
            "backend":             sess.get("exec_backend", "cloudshell"),
            "identityfiles":       idf_map,
            "default_identityfile": idf_default,
        }

        # ── disk snapshot from watch ──────────────────────
        disk = {}
        try:
            if _DISK_STATE_FILE.exists():
                disk = json.loads(_DISK_STATE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            disk = {"errors": [f"read disk_state failed: {e}"]}

        return {
            "login":    login,
            "server":   server,
            "watch":    watch,
            "autologin": autologin,
            "exec":     exec_info,
            "disk":     disk,
        }

    def _render_health(data: dict, last_run_ts: float, jobs: list, browser: bool) -> str:
        import datetime as _dt
        tz = _dt.timezone(_dt.timedelta(hours=8))
        now_str = _dt.datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S CST")

        login = data.get("login",     {})
        watch = data.get("watch",     {})
        al    = data.get("autologin", {})
        disk  = data.get("disk",      {})

        logged_in = login.get("logged_in", False)
        age_h     = login.get("session_age_hours")
        watch_on  = watch.get("enabled", False)
        has_pend  = any(j.get("status") == "Pending" for j in jobs)
        iv_h      = watch.get("interval_h")
        lc        = watch.get("last_check")
        al_on     = al.get("enabled", False)
        channel   = al.get("otp_channel", "none")
        al_ts     = al.get("last_autologin_ts", 0)
        tripped   = al.get("circuit_tripped", False)
        failures  = al.get("consecutive_failures", 0)
        threshold = al.get("circuit_breaker", 3)

        def dur(ts: float) -> str:
            diff = time.time() - ts
            if diff < 60:    return f"{int(diff)}s"
            if diff < 3600:  return f"{int(diff / 60)}m"
            if diff < 86400: return f"{diff / 3600:.1f}h"
            return f"{diff / 86400:.1f}d"

        def _as_int(value, default: int = 0) -> int:
            try:
                return int(value)
            except Exception:
                return default

        def disk_evict_limit(host: dict) -> int:
            df = host.get("df") or {}
            return int(_as_int(df.get("total_bytes")) * 0.9)

        def disk_alloc_ratio(host: dict) -> float:
            df = host.get("df") or {}
            used = _as_int(df.get("used_bytes"))
            limit = disk_evict_limit(host)
            return used / limit if limit > 0 else 0.0

        def disk_has_large_job(host: dict) -> bool:
            df = host.get("df") or {}
            used = _as_int(df.get("used_bytes"))
            if used <= 0:
                return False
            return any(
                _as_int(job.get("cache_bytes")) / used > 0.10
                for job in host.get("jobs", [])
                if job.get("cache_bytes") is not None
            )

        def disk_attention_hosts():
            return [
                h for h in sorted_disk_hosts()
                if disk_alloc_ratio(h) > 0.50 and disk_has_large_job(h)
            ]

        def fmt_disk_bytes(value, threshold: bool = False) -> str:
            n = float(_as_int(value))
            units = [
                (1024 ** 4, "TB"),
                (1024 ** 3, "GB"),
                (1024 ** 2, "MB"),
                (1024, "KB"),
            ]
            for factor, suffix in units:
                if n >= factor:
                    v = n / factor
                    if suffix == "TB":
                        digits = 2 if threshold else 1
                    elif suffix == "KB" and v >= 10:
                        digits = 0
                    else:
                        digits = 1
                    text = f"{v:.{digits}f}".rstrip("0").rstrip(".")
                    return f"{text}{suffix}"
            return f"{int(n)}B"

        def disk_job_list(host: dict, port_color: str = "", reset: str = "") -> str:
            parts = []
            for job in sorted(host.get("jobs", []), key=lambda j: j.get("port") or 0):
                port = job.get("port", "—")
                port_text = f"{port_color}{port}{reset}" if port_color else str(port)
                cache = fmt_disk_bytes(job.get("cache_bytes")) if job.get("cache_bytes") is not None else "—"
                parts.append(f"{port_text} {cache}")
            return ", ".join(parts) if parts else "no jobs"

        def disk_host_line(host: dict, value_color: str = "", accent_color: str = "", reset: str = "") -> str:
            df = host.get("df") or {}
            used = _as_int(df.get("used_bytes"))
            total = _as_int(df.get("total_bytes"))
            total_tb_display = round(total / (1024 ** 4), 1) if total > 0 else 0.0
            limit_text = f"{total_tb_display * 0.9:.2f}TB" if total_tb_display else "—"
            host_ip = host.get("host_ip", "UNKNOWN")
            usage = f"{fmt_disk_bytes(used)} / {limit_text}"
            if value_color:
                usage = f"{value_color}{usage}{reset}"
            host_ip_text = f"{accent_color}{host_ip}{reset}" if accent_color else host_ip
            return (
                f"{usage} {host_ip_text} "
                f"({disk_job_list(host, accent_color, reset)})"
            )

        def disk_checked_ts(raw: str):
            if not raw:
                return None
            try:
                return _dt.datetime.fromisoformat(raw.rstrip("Z")).replace(tzinfo=_dt.timezone.utc).timestamp()
            except Exception:
                return None

        def sorted_disk_hosts():
            hosts = (disk or {}).get("hosts", {}) or {}
            return sorted(hosts.values(), key=disk_alloc_ratio, reverse=True)

        if browser:
            # ── 详情模式（无 ANSI 色彩，供浏览器显示）──────────────
            def cst(ts: float) -> str:
                return _dt.datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S CST")

            def cst_iso(s: str) -> str:
                dt = _dt.datetime.fromisoformat(s.rstrip("Z")).replace(tzinfo=_dt.timezone.utc)
                return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S CST")

            def ago(ts: float) -> str:
                diff = time.time() - ts
                if diff < 60:    return f"{int(diff)}s ago"
                if diff < 3600:  return f"{int(diff / 60)}m ago"
                if diff < 86400: return f"{diff / 3600:.1f}h ago"
                return f"{diff / 86400:.1f}d ago"

            def dot(ok: bool) -> str: return "● " if ok else "○ "

            lines = []
            lines.append(f"macli Health  {now_str}")
            lines.append("")

            lines.append("Login")
            lines.append(f"  Status       {dot(logged_in)}{'logged in' if logged_in else 'NOT logged in'}")
            if login.get("user"):
                lines.append(f"  User         {login['user']}  {login.get('domain', '')}")
            if age_h is not None:
                lines.append(f"  Session age  {age_h}h")
            lines.append("")

            lines.append("GPU")
            if last_run_ts > 0:
                lines.append(f"  Last query   {ago(last_run_ts)}  ({cst(last_run_ts)})")
            else:
                lines.append(f"  Last query   never")
            if logged_in:
                from collections import Counter as _Counter
                ph   = _Counter(j.get("status", "") for j in jobs)
                run  = ph.get("Running",    0)
                pend = ph.get("Pending",    0)
                term = ph.get("Terminated", 0) + ph.get("Stopped", 0)
                fail = ph.get("Failed",     0)
                if jobs:
                    if run:  lines.append(f"  ● Running     {run}")
                    if pend: lines.append(f"  ● Pending     {pend}")
                    if term: lines.append(f"  ● Terminated  {term}")
                    if fail: lines.append(f"  ● Failed      {fail}")
                else:
                    lines.append(f"  (no jobs)")
            lines.append("")

            if not watch_on:
                w_label = "disabled"
            elif has_pend:
                w_label = "enabled"
            else:
                w_label = "enabled  (检查暂未及时发生)"
            lines.append(f"Watch  {dot(watch_on)}{w_label}")
            if iv_h:
                lines.append(f"  Interval     every {iv_h}h")
            if lc:
                try:    lines.append(f"  Last run     {cst_iso(lc)}")
                except: lines.append(f"  Last run     {lc}")
            else:
                lines.append(f"  Last run     never")
            lines.append("")

            lines.append(f"Autologin  {dot(al_on)}{'enabled' if al_on else 'disabled'}  [{channel}]")
            if al_ts:
                lines.append(f"  Last login   {ago(al_ts)}  ({cst(al_ts)})")
            else:
                lines.append(f"  Last login   never")
            if tripped:
                lines.append(f"  Circuit      {dot(False)}tripped  ({failures}/{threshold} failures)")
            else:
                lines.append(f"  Circuit      {dot(True)}normal  ({failures}/{threshold} failures)")
            lines.append("")

            if disk:
                ts = disk_checked_ts(disk.get("last_check"))
                age = dur(ts) if ts else "never"
                lines.append(
                    f"Disk {age} hosts {len((disk.get('hosts') or {}))} "
                    f"jobs {disk.get('jobs_checked', 0)}"
                )

                attention = disk_attention_hosts()
                for host in attention:
                    lines.append(disk_host_line(host))
                if not attention:
                    lines.append("(no hosts above filters)")

                lines.append("")
                lines.append("All hosts")
                for host in sorted_disk_hosts():
                    lines.append(disk_host_line(host))

                if ts:
                    lines.append("")
                    lines.append(f"Last scan {cst(ts)}")
                elif disk.get("last_check"):
                    lines.append("")
                    lines.append(f"Last scan {disk.get('last_check')}")
                for err in disk.get("errors", []) or []:
                    lines.append(f"Error {err}")
                lines.append("")

            return "\n".join(lines)

        # ── 紧凑模式（ANSI 彩色，供 terminal/curl 显示）─────────
        B   = "\033[1m"
        R   = "\033[0m"
        G   = "\033[32m"
        Y   = "\033[33m"
        RED = "\033[31m"
        GR  = "\033[90m"
        DIM = "\033[2m"
        ORANGE = "\033[38;5;214m"
        BLUE = "\033[94m"

        def dot(ok: bool) -> str:
            return f"{G}●{R}" if ok else f"{RED}●{R}"

        age_str = f" {age_h}h" if age_h is not None else ""
        line1 = f"{B}macli{R}  {dot(logged_in)}{age_str}  {DIM}{now_str}{R}"

        gpu_str = f"GPU {dur(last_run_ts)}" if last_run_ts > 0 else f"GPU {DIM}never{R}"
        if logged_in:
            from collections import Counter as _Counter
            ph   = _Counter(j.get("status", "") for j in jobs)
            run  = ph.get("Running",    0)
            pend = ph.get("Pending",    0)
            term = ph.get("Terminated", 0) + ph.get("Stopped", 0)
            fail = ph.get("Failed",     0)
            gpu_str += f"  {G}{run}{R}│{Y}{pend}{R}│{GR}{term}{R}│{RED}{fail}{R}"

        lc_str = ""
        if lc:
            try:
                dt     = _dt.datetime.fromisoformat(lc.rstrip("Z")).replace(tzinfo=_dt.timezone.utc)
                lc_str = dur(dt.timestamp())
            except Exception:
                pass

        if not watch_on:
            wd, w_warn = dot(False), ""
        elif has_pend:
            wd, w_warn = f"{G}●{R}", ""
        else:
            wd, w_warn = f"{Y}●{R}", ""

        iv_part   = f" {iv_h}h" if iv_h else ""
        lc_part   = f"  {lc_str}" if lc_str else ""
        watch_str = f"Watch {wd}{w_warn}{iv_part}{lc_part}"
        line2     = f"{gpu_str}   {watch_str}"

        al_last  = f"  {dur(al_ts)}" if al_ts else ""
        cd       = dot(False) if tripped else dot(True)
        c_label  = f"tripped {failures}/{threshold}" if tripped else f"{failures}/{threshold}"
        line3    = f"Autologin {dot(al_on)} {channel}{al_last}  {cd} {c_label}"

        lines = [line1, line2, line3]
        if disk:
            ts = disk_checked_ts(disk.get("last_check"))
            age = dur(ts) if ts else "never"
            lines.append(
                f"Disk {age} hosts {len((disk.get('hosts') or {}))} "
                f"jobs {disk.get('jobs_checked', 0)}"
            )

            def disk_value_color(host: dict) -> str:
                ratio = disk_alloc_ratio(host)
                if ratio >= 0.85:
                    return RED
                if ratio >= 0.70:
                    return ORANGE
                if ratio >= 0.50:
                    return Y
                return ""

            attention = disk_attention_hosts()
            for host in attention:
                lines.append(disk_host_line(host, disk_value_color(host), BLUE, R))
            if not attention:
                lines.append(f"{DIM}(no hosts above filters){R}")

        return "\n".join(lines) + "\n"

    @app.get("/health.json")
    def health():
        data, _ = _health_cache.get(_fetch_health)
        with _cache_lock:
            last = _cache["last_run"]
        gpu_age = round(time.monotonic() - last, 1) if last > 0 else None
        return {"status": "ok", "port": port, "gpu_cache_age_s": gpu_age, **data}

    @app.get("/health", response_class=_Plain)
    def health_human(req: _Request):
        data, _    = _health_cache.get(_fetch_health)
        jobs, _    = _jobs_cache.get(_fetch_jobs_for_health)
        with _cache_lock:
            last_run_ts = _cache.get("last_run_ts", 0.0)
        browser = _is_browser(req)
        return _Plain(_render_health(data, last_run_ts, jobs, browser))

    @app.get("/watch-log", response_class=_Plain)
    def get_watch_log():
        _watch_log = Path(os.environ.get(
            "XDG_CONFIG_HOME", Path.home() / ".config"
        )) / "macli" / "watch.log"
        return _Plain(_tail(_watch_log, 1000))

    @app.get("/ports")
    def get_ports():
        def _fetch_ports():
            r = _subprocess.run(
                [sys.executable, "-m", "macli", "ports", "--json"],
                capture_output=True, text=True, timeout=60,
            )
            if r.returncode == 0:
                try:
                    return json.loads(r.stdout)
                except json.JSONDecodeError:
                    pass
            return []

        data, age = _ports_cache.get(_fetch_ports)
        enriched = []
        for r in data:
            row = dict(r)
            if "quota_class" not in row:
                row.update({
                    "quota_class": "unknown",
                    "quota_labels": ["unknown", "preemptible"],
                    "quota_reason": "quota metadata unavailable",
                })
            if "preemptible" not in row:
                row["preemptible"] = row.get("quota_class") != "guaranteed"
            enriched.append(row)
        from fastapi.responses import JSONResponse as _JResp
        return _JResp(content=enriched,
                      headers={"X-Cache-Age": str(age)})

    # ── OTP webhook endpoints ────────────────────────────────
    @app.post("/otp")
    async def recv_otp(req: _Request):
        body = (await req.body()).decode(errors="replace").strip()
        # 支持 JSON {"code":"123456"} 或纯文本
        try:
            data = json.loads(body)
            text = str(data.get("code") or data.get("message") or "").strip()
        except Exception:
            text = body
        m = re.search(r"\b(\d{6})\b", text)
        if not m:
            return _JSON({"ok": False, "error": "no 6-digit code found"}, status_code=400)
        _otp_slot["code"] = m.group(1)
        _otp_slot["expires"] = time.time() + 90
        _otp_slot["event"].set()
        return _JSON({"ok": True})

    @app.get("/otp/wait")
    async def wait_otp(timeout: int = 120):
        _otp_slot["event"].clear()
        try:
            await _asyncio.wait_for(_otp_slot["event"].wait(), timeout=timeout)
        except _asyncio.TimeoutError:
            return _JSON({"ok": False, "error": "timeout"}, status_code=408)
        if time.time() > _otp_slot["expires"]:
            return _JSON({"ok": False, "error": "code expired"}, status_code=410)
        code = _otp_slot["code"]
        _otp_slot["event"].clear()
        return _JSON({"ok": True, "code": code})

    cprint(f"[cyan]macli server  http://0.0.0.0:{port}[/cyan]")
    for route in ("/gpu", "/gpu.json", "/ports", "/log", "/watch-log", "/server-log", "/health", "/otp"):
        cprint(f"  http://localhost:{port}{route}")
    _uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
