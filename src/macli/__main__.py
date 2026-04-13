#!/usr/bin/env python3
"""
华为云 ModelArts 远程管理 CLI
author: @hammershock
version: 0.0.1
"""
import os, sys, json, time, re, copy, argparse, subprocess as _subprocess, tempfile, shutil, ssl, socket, base64, struct, urllib.parse, threading, tty, termios, select, signal
from pathlib import Path
from datetime import datetime, timezone, timedelta
_CST = timezone(timedelta(hours=8))


def _ensure_pkg(*packages: str) -> None:
    """检查包是否已安装，缺少则自动 pip install 一次，然后重启脚本。
    用环境变量 _MACLI_INSTALLED 标记已尝试安装，避免安装失败后无限重启。
    """
    import importlib
    missing = []
    for pkg in packages:
        import_name = {"Pillow": "PIL", "scikit-learn": "sklearn"}.get(pkg, pkg)
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(pkg)
    if not missing:
        return
    if os.environ.get("_MACLI_INSTALLED"):
        print(f"[macli] 错误：依赖 {', '.join(missing)} 安装后仍无法导入。")
        print(f"[macli] 请手动执行：pip install {' '.join(missing)}")
        sys.exit(1)
    print(f"[macli] 缺少依赖：{', '.join(missing)}，正在安装...")
    try:
        _subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", *missing]
        )
    except _subprocess.CalledProcessError:
        print(f"[macli] 安装失败，请手动执行：pip install {' '.join(missing)}")
        sys.exit(1)
    print("[macli] 安装完成，重新启动...")
    env = os.environ.copy()
    env["_MACLI_INSTALLED"] = "1"
    os.execve(sys.executable, [sys.executable] + sys.argv, env)


_ensure_pkg("requests", "rich")

# keyring 用于将账号密码存入系统安全存储（macOS Keychain / Linux Secret Service / Windows Credential Manager）
# 不强依赖：若安装失败或平台不支持，自动退化为不保存密码
try:
    import keyring as _keyring
    _keyring.get_password("macli", "_probe")  # 探测后端是否可用
    _KEYRING_OK = True
except Exception:
    _keyring = None
    _KEYRING_OK = False

_KR_SERVICE = "macli"
_KR_KEY     = "credentials"

import requests
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn

try:
    from rich.syntax import Syntax
except Exception:
    Syntax = None


class SessionExpiredError(Exception):
    """登录凭据已过期，需要重新登录"""
    pass
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm
console = Console()

# region id → 中文名（硬编码于华为云控制台前端）
REGION_NAMES = {
    "af-north-1":     "非洲-开罗",
    "af-south-1":     "非洲-约翰内斯堡",
    "ap-southeast-1": "中国-香港",
    "ap-southeast-2": "亚太-曼谷",
    "ap-southeast-3": "亚太-新加坡",
    "ap-southeast-4": "亚太-雅加达",
    "ap-southeast-5": "亚太-马尼拉",
    "cn-east-3":      "华东-上海一",
    "cn-east-4":      "华东二",
    "cn-east-5":      "华东-青岛",
    "cn-north-4":     "华北-北京四",
    "cn-north-5":     "华北-乌兰察布二零一",
    "cn-north-6":     "华北-乌兰察布二零二",
    "cn-north-9":     "华北-乌兰察布一",
    "cn-north-11":    "华北三",
    "cn-north-12":    "华北三",
    "cn-south-1":     "华南-广州",
    "cn-south-4":     "华南-广州-友好用户环境",
    "cn-southwest-2": "西南-贵阳一",
    "eu-west-0":      "欧洲-巴黎",
    "la-north-2":     "拉美-墨西哥城二",
    "la-south-2":     "拉美-圣地亚哥",
    "me-east-1":      "中东-利雅得",
    "sa-brazil-1":    "拉美-圣保罗一",
}
CONSOLE_BASE = "https://console.huaweicloud.com"

def _new_session() -> requests.Session:
    s = requests.Session()
    s.proxies = {"http": "", "https": "", "no_proxy": "*"}
    s.trust_env = False  # 不从环境变量读代理
    return s

def _config_path() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "macli" / "session.json"

# ── 工具函数 ──────────────────────────────────────────────────

_VERBOSE = False  # --debug 时设为 True

# ── 文件日志 ──────────────────────────────────────────────────
_LOG_PATH: "Path | None" = None
_RICH_TAG_RE = re.compile(r'\[[^\]\n]*?\]')

def _strip_rich(s: str) -> str:
    """去除 Rich 样式标签，保留纯文本内容。"""
    return _RICH_TAG_RE.sub('', s)

def _flog(level: str, msg: str):
    """向日志文件追加一条记录。忽略 IO 错误以免影响主流程。"""
    if _LOG_PATH is None:
        return
    ts = datetime.now(_CST).strftime("%Y-%m-%dT%H:%M:%S+08")
    line = f"{level}: {ts}: {_strip_rich(str(msg)).strip()}\n"
    try:
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass

def _init_logger():
    """初始化日志文件路径（在 _main_impl 开始前调用一次）。"""
    global _LOG_PATH
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    _LOG_PATH = base / "macli" / "macli.log"
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        _LOG_PATH = None  # 目录创建失败则禁用文件日志

def cprint(msg, style=None):
    console.print(msg, style=style)
    s = str(msg)
    if "[red]" in s or "[bold red]" in s:
        _flog("ERROR", s)
    elif "[yellow]" in s:
        _flog("WARN", s)
    else:
        _flog("INFO", s)

def dprint(msg, style=None):
    """始终记录到日志文件（DEBUG 级别）；仅 --debug 模式下输出到控制台。"""
    _flog("DEBUG", str(msg))
    if _VERBOSE:
        console.print(msg, style=style)

def _raw_debug(msg: str):
    """raw tty 模式下向 stderr 输出一行调试信息（\\r\\n 避免阶梯错位）。"""
    if _VERBOSE:
        os.write(sys.stderr.fileno(), f"\r\033[K\033[2m[dbg] {msg}\033[m\r\n".encode())

def _status_debug(msg: str):
    """将调试状态锁定在终端第一行原地刷新，不滚屏（光标 save/restore）。"""
    if _VERBOSE:
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 80
        short = msg[:cols - 2]
        os.write(sys.stderr.fileno(),
                 f"\033[s\033[1;1H\033[K\033[2m{short}\033[m\033[u".encode())

def load_session() -> dict:
    """从 config/session.json 读取 session"""
    try:
        p = _config_path()
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_session(data: dict):
    """将 session 写入 config/session.json"""
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── SSH 密钥管理 ──────────────────────────────────────────────

def load_identityfiles() -> tuple:
    """返回 (files_dict, default_name)，files_dict 为 {name: path}。"""
    data = load_session()
    return data.get("identityfiles", {}), data.get("default_identityfile", None)


def save_identityfiles(files: dict, default: str = None):
    """将 identityfiles 写回 session.json。"""
    data = load_session()
    data["identityfiles"] = files
    data["default_identityfile"] = default
    save_session(data)


def get_exec_backend() -> str:
    """返回已保存的 exec 后端，默认 cloudshell。"""
    return load_session().get("exec_backend", "cloudshell")


def set_exec_backend(backend: str):
    """持久化 exec 后端选择。"""
    data = load_session()
    data["exec_backend"] = backend
    save_session(data)


def resolve_identityfile(name_or_path: str) -> str:
    """将名称或路径解析为实际文件路径。
    - 若含路径分隔符或以 . 开头，视为路径直接使用
    - 否则在已保存的密钥列表中按名称查找
    - 找不到则原样返回（交由 SSH 自行报错）
    """
    if not name_or_path:
        return name_or_path
    if os.sep in name_or_path or name_or_path.startswith(".") or name_or_path.startswith("~"):
        return str(Path(name_or_path).expanduser())
    files, _ = load_identityfiles()
    if name_or_path in files:
        return str(Path(files[name_or_path]).expanduser())
    # 可能就是文件名（相对路径），直接返回
    return name_or_path


def _parse_ssh_url(url: str):
    """从 ssh://user@host:port 中提取 (user, host, port)，失败返回 (None, None, None)。"""
    if not url:
        return None, None, None
    m = re.match(r"^ssh://([^@]+)@([^:]+):(\d+)$", url.strip())
    if m:
        return m.group(1), m.group(2), int(m.group(3))
    return None, None, None


# ── 凭据安全存储（系统 Keychain）────────────────────────────────

_CREDS_FILE = _config_path().parent / "credentials.json"


def _load_saved_creds() -> dict:
    """从 Keychain 或备用文件读取已保存的账号密码"""
    if _KEYRING_OK:
        try:
            raw = _keyring.get_password(_KR_SERVICE, _KR_KEY)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    # 回退：明文文件
    try:
        if _CREDS_FILE.exists():
            return json.loads(_CREDS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_creds(domain: str, username: str, password: str) -> bool:
    """将账号密码存入 Keychain（优先）或备用明文文件"""
    payload = json.dumps({"domain": domain, "username": username, "password": password},
                         ensure_ascii=False)
    if _KEYRING_OK:
        try:
            _keyring.set_password(_KR_SERVICE, _KR_KEY, payload)
            return True
        except Exception:
            pass
    # 回退：明文文件（权限 600）
    try:
        _CREDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CREDS_FILE.write_text(payload, encoding="utf-8")
        _CREDS_FILE.chmod(0o600)
        dprint("[dim]密码已存入文件（keyring 不可用）[/dim]")
        return True
    except Exception:
        return False


def _clear_saved_creds() -> bool:
    """删除已保存的账号密码"""
    ok = False
    if _KEYRING_OK:
        try:
            _keyring.delete_password(_KR_SERVICE, _KR_KEY)
            ok = True
        except Exception:
            pass
    if _CREDS_FILE.exists():
        try:
            _CREDS_FILE.unlink()
            ok = True
        except Exception:
            pass
    return ok


# ── 自动登录 ──────────────────────────────────────────────────

_AUTOLOGIN_KEY = "auto_login"


def _load_auto_login_cfg() -> dict:
    """从 session.json 读取自动登录配置，返回 dict（未配置时返回 {}）"""
    return load_session().get(_AUTOLOGIN_KEY, {})


def _save_auto_login_cfg(cfg: dict):
    """将自动登录配置写回 session.json"""
    data = load_session()
    data[_AUTOLOGIN_KEY] = cfg
    save_session(data)


def _ntfy_poll_otp(topic: str, since_ts: int, timeout: int = 120) -> str:
    """
    轮询 ntfy.sh/{topic}，返回第一条在 since_ts 之后发布的 6 位纯数字消息体。
    超时或失败时返回空字符串。
    """
    deadline = time.monotonic() + timeout
    url = f"https://ntfy.sh/{topic}/json"
    seen_ids: set = set()
    dprint(f"[dim]ntfy 轮询开始  url={url}  since={since_ts}[/dim]")
    while time.monotonic() < deadline:
        try:
            r = requests.get(url, params={"poll": "1", "since": str(since_ts)}, timeout=10)
            dprint(f"[dim]ntfy 响应 HTTP {r.status_code}  body={r.text[:120]!r}[/dim]")
            if r.status_code == 200:
                for line in r.text.strip().splitlines():
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line)
                    except Exception:
                        continue
                    mid  = msg.get("id", "")
                    body = (msg.get("message") or "").strip()
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    # 快捷指令有时将 {"message":"123456"} 作为纯文本发送，需要解包
                    if body.startswith("{"):
                        try:
                            inner = json.loads(body)
                            body = (inner.get("message") or body).strip()
                        except Exception:
                            pass
                    dprint(f"[dim]ntfy 消息 id={mid!r} body={body!r}[/dim]")
                    # 支持完整短信原文，从中提取首个 6 位数字
                    m = re.search(r"\b(\d{6})\b", body)
                    if m:
                        return m.group(1)
        except Exception as ex:
            dprint(f"[dim]ntfy 轮询异常: {ex}[/dim]")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(3.0, remaining))
    return ""


def _webhook_poll_otp(webhook_url: str, timeout: int = 120) -> str:
    """
    长轮询本地 macli server /otp/wait 端点，返回 6 位验证码。
    超时或失败时返回空字符串。
    """
    url = webhook_url.rstrip("/") + "/otp/wait"
    dprint(f"[dim]webhook 轮询开始  url={url}  timeout={timeout}[/dim]")
    try:
        r = requests.get(url, params={"timeout": timeout}, timeout=timeout + 10)
        dprint(f"[dim]webhook 响应 HTTP {r.status_code}  body={r.text[:120]!r}[/dim]")
        if r.status_code == 200:
            data = r.json()
            if data.get("ok") and data.get("code"):
                return data["code"]
    except Exception as ex:
        dprint(f"[dim]webhook 轮询异常: {ex}[/dim]")
    return ""


def _do_auto_login(cfg: dict) -> bool:
    """
    用 keyring 中存储的账号密码 + webhook/ntfy OTP 通道自动完成登录，
    成功后更新 session.json 并返回 True，失败返回 False。
    """
    creds = _load_saved_creds()
    if not (creds.get("domain") and creds.get("username") and creds.get("password")):
        cprint("[red]自动登录失败：keyring 中无账号密码，请先执行 macli autologin enable[/red]")
        return False

    webhook_url = cfg.get("webhook_url", "")
    ntfy_topic  = cfg.get("ntfy_topic", "")
    max_retries = int(cfg.get("max_retries", 3))
    otp_timeout = int(cfg.get("otp_wait_secs", 120))

    if not webhook_url and not ntfy_topic:
        cprint("[red]自动登录失败：未配置 webhook_url 或 ntfy_topic，请执行 macli autologin enable[/red]")
        return False

    otp_mode = "webhook" if webhook_url else "ntfy"
    cprint(
        f"\n[bold cyan]⟳ 会话已过期，自动重新登录[/bold cyan]"
        f"  [dim]{creds['username']} @ {creds['domain']}  (OTP: {otp_mode})[/dim]"
    )

    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            cprint(f"[yellow]第 {attempt}/{max_retries} 次重试...[/yellow]")

        # 捕获 poll_since 在闭包外，每次尝试都重新记录
        poll_since = int(time.time())

        if webhook_url:
            def _otp_provider() -> str:
                dprint(f"[dim]webhook 开始轮询，url={webhook_url}，timeout={otp_timeout}s[/dim]")
                cprint(f"[cyan]  ⟳ 等待手机验证码（最多 {otp_timeout} 秒）...[/cyan]")
                code = _webhook_poll_otp(webhook_url, timeout=otp_timeout)
                if not code:
                    cprint("[yellow]  验证码等待超时[/yellow]")
                else:
                    dprint(f"[dim]webhook 收到验证码: {code}[/dim]")
                return code
        else:
            def _otp_provider(since=poll_since) -> str:
                dprint(f"[dim]ntfy 开始轮询，since={since}，topic={ntfy_topic}，timeout={otp_timeout}s[/dim]")
                cprint(f"[cyan]  ⟳ 等待手机验证码（最多 {otp_timeout} 秒）...[/cyan]")
                code = _ntfy_poll_otp(ntfy_topic, since, timeout=otp_timeout)
                if not code:
                    cprint("[yellow]  验证码等待超时[/yellow]")
                else:
                    dprint(f"[dim]ntfy 收到验证码: {code}[/dim]")
                return code

        ck, http_s = _http_login(
            creds["domain"], creds["username"], creds["password"],
            otp_provider=_otp_provider,
        )
        if ck:
            _setup_session_from_cookie(ck, interactive=False, http_session=http_s)
            cprint("[bold green]✓ 自动重新登录成功[/bold green]")
            return True
        cprint(f"[yellow]第 {attempt} 次尝试失败[/yellow]")

    cprint("[red]✗ 自动重新登录失败，已超过最大重试次数[/red]")
    return False


def _autologin_record_outcome(success: bool) -> bool:
    """
    更新连续失败计数器。
    成功时重置为 0；失败时递增，达到熔断阈值则自动禁用 autologin。
    返回 True 表示本次触发了熔断（autologin 刚被禁用）。
    """
    data = load_session()
    cfg  = data.get(_AUTOLOGIN_KEY, {})
    if success:
        if cfg.get("consecutive_failures", 0) != 0:
            cfg["consecutive_failures"] = 0
            cfg["circuit_tripped"]      = False
            data[_AUTOLOGIN_KEY] = cfg
            save_session(data)
        return False
    threshold = int(cfg.get("circuit_breaker", 3))
    failures  = int(cfg.get("consecutive_failures", 0)) + 1
    cfg["consecutive_failures"] = failures
    if failures >= threshold:
        cfg["enabled"]         = False
        cfg["circuit_tripped"] = True
        data[_AUTOLOGIN_KEY]   = cfg
        save_session(data)
        _flog("ERROR",
              f"autologin 熔断：连续失败 {failures} 次（阈值 {threshold}），已自动禁用")
        return True
    data[_AUTOLOGIN_KEY] = cfg
    save_session(data)
    _flog("WARN", f"autologin 失败（连续 {failures}/{threshold} 次）")
    return False


class PortCache:
    """
    Running 状态 job 的 SSH 端口缓存（线程安全写入）。

    规则：
    - 只有 Running 状态的 job 才可能持有 SSH 端口
    - 端口在 job 运行期间固定不变，结束时失效
    - job 一旦不再 Running，立即从缓存中驱逐
    - 持久化到 session.json["ssh_port_cache"]
    """
    _SESSION_KEY = "ssh_port_cache"

    def __init__(self):
        self._data: dict = {}   # {job_id: [{task, url, port}, ...]}
        self._dirty = False
        self._lock = threading.Lock()

    def load(self) -> "PortCache":
        self._data = load_session().get(self._SESSION_KEY, {})
        self._dirty = False
        return self

    def save(self):
        if not self._dirty:
            return
        with self._lock:
            d = load_session()
            d[self._SESSION_KEY] = self._data
            save_session(d)
            self._dirty = False

    def get(self, job_id: str):
        """缓存命中返回 ssh entries 列表；未命中返回 None。"""
        return self._data.get(job_id)

    def put(self, job_id: str, entries: list):
        """写入非空 entries（线程安全）。"""
        if not entries:
            return
        with self._lock:
            self._data[job_id] = entries
            self._dirty = True

    def evict(self, job_id: str):
        with self._lock:
            if job_id in self._data:
                del self._data[job_id]
                self._dirty = True

    def evict_non_running(self, running_ids: set) -> list:
        """清除不在 running_ids 中的所有缓存条目，返回被清除的 ID 列表。"""
        with self._lock:
            stale = [k for k in list(self._data) if k not in running_ids]
            for k in stale:
                del self._data[k]
            if stale:
                self._dirty = True
        return stale


def resolve_ssh(api: "API", job_id: str, phase: str,
                cache: "PortCache", detail_hint: dict = None) -> list:
    """
    获取 job 的 SSH entries（带缓存）。

    - 非 Running 状态：驱逐缓存，返回 []
    - Running + 缓存命中：直接返回缓存
    - Running + 缓存未命中：用 detail_hint 或拉取 detail，有端口则写缓存
    """
    if phase != "Running":
        cache.evict(job_id)
        return []
    cached = cache.get(job_id)
    if cached is not None:
        return cached
    detail = detail_hint or api.get_job(job_id)
    if not detail:
        return []
    entries = enrich_ssh_entries(
        detail.get("endpoints", {}).get("ssh", {}).get("task_urls", [])
    )
    cache.put(job_id, entries)
    return entries


def parse_recent(s: str):
    """解析 --recent 参数，返回毫秒数。格式: 4d / 5h / 3m(月) / 1y"""
    if not s:
        return None
    s = s.strip().lower()
    import re as _re
    m = _re.match(r'^(\d+)(h|d|m|y)$', s)
    if not m:
        raise ValueError(f"无法解析时间格式: {s!r}，支持: 4d / 5h / 3m / 1y")
    n, unit = int(m.group(1)), m.group(2)
    seconds = {"h": 3600, "d": 86400, "m": 86400 * 30, "y": 86400 * 365}[unit]
    return n * seconds * 1000  # 转毫秒


def job_to_dict(j: dict, ssh_override: list = None) -> dict:
    """将 API 返回的 job 对象提炼为简洁的可序列化字典。
    ssh_override: 若传入，用此值替代 j 中的 endpoints.ssh（用于缓存注入场景）。
    """
    meta = j.get("metadata", {})
    st   = j.get("status",   {})
    spec = j.get("spec",     {})
    res  = spec.get("resource", {})
    if ssh_override is not None:
        ssh = ssh_override
    else:
        ssh = enrich_ssh_entries(j.get("endpoints", {}).get("ssh", {}).get("task_urls", []))
    return {
        "id":          meta.get("id", ""),
        "name":        meta.get("name", ""),
        "status":      st.get("phase", ""),
        "duration_ms": st.get("duration"),
        "duration":    ms_to_hms(st.get("duration")),
        "gpu_count":   res.get("pool_info", {}).get("accelerator_num"),
        "flavor_id":   res.get("flavor_id", ""),
        "image":       j.get("algorithm", {}).get("engine", {}).get("image_url", ""),
        "create_time": meta.get("create_time"),
        "create_time_str": ts_to_str(meta.get("create_time")),
        "user_name":   meta.get("user_name", ""),
        "description": meta.get("description", ""),
        "ssh":         ssh,
    }


def _json_out(data):
    """输出 JSON 到 stdout"""
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _fmt_flavor(resource: dict) -> str:
    """目标规格：来自 pool_info"""
    p   = resource.get("pool_info", {})
    gpu = p.get("accelerator_num", "?")
    acc = p.get("accelerator_type", "?")   # nvidia-a100-nv80
    cpu = p.get("core_num", "?")
    mem = p.get("mem_size", "?")
    return f"{gpu}*{acc} | {cpu}vCPUs | {mem}GiB"

def _fmt_actual(resource: dict) -> str:
    """实际分配：来自 main_container_allocated_resources"""
    a = resource.get("main_container_allocated_resources", {})
    if not a:
        return "(未分配)"
    gpu = a.get("accelerator_num", "?")
    acc = a.get("accelerator_type", "?")
    cpu = a.get("cpu_core_num", "?")
    mem = a.get("mem_size", "?")
    return f"{gpu}*{acc} | {cpu}vCPUs | {mem}GiB"

def ms_to_hms(ms):
    if not ms: return "--"
    try:
        h, rem = divmod(int(ms) // 1000, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    except: return str(ms)

def ts_to_str(ts):
    if not ts: return "--"
    try: return datetime.fromtimestamp(int(ts) / 1000, tz=_CST).strftime("%Y-%m-%d %H:%M")
    except: return str(ts)


def ssh_url_to_port(url: str):
    """从 ssh://user@host:port URL 中提取端口号；取不到则返回 None。"""
    if not url or not isinstance(url, str):
        return None
    m = re.match(r"^ssh://[^@]+@[^:]+:(\d+)$", url.strip())
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def enrich_ssh_entries(entries: list) -> list:
    """为 SSH 条目补充 port 字段。"""
    out = []
    for item in entries or []:
        url = item.get("url", "")
        out.append({
            "task": item.get("task", ""),
            "url": url,
            "port": ssh_url_to_port(url),
        })
    return out


def ssh_ports_list(entries: list) -> list:
    """提取并去重 SSH 端口列表，返回 int 列表。"""
    ports = []
    for item in entries or []:
        port = item.get("port")
        if port is None:
            port = ssh_url_to_port(item.get("url", ""))
        if port is not None:
            ports.append(int(port))
    return list(dict.fromkeys(ports))



def ssh_ports_summary(entries: list) -> str:
    """用于表格展示 SSH 端口。多个端口去重后以逗号连接；缺失返回 —。"""
    ports = ssh_ports_list(entries)
    return ",".join(map(str, ports)) if ports else "—"

# ── Session ──────────────────────────────────────────────────

class ConsoleSession:

    def __init__(self):
        self.http         = _new_session()
        self.project_id   = None
        self.region       = None
        self.agency_id    = None
        self.workspace_id = None
        self.cftk         = None

    def init(self, cookie_str, region, project_id, agency_id, cftk, workspace_id=""):
        self.region       = region
        self.project_id   = project_id
        self.agency_id    = agency_id
        self.cftk         = cftk
        self.workspace_id = workspace_id
        for part in cookie_str.split(";"):
            k, _, v = part.strip().partition("=")
            if k: self.http.cookies.set(k.strip(), v.strip())
        self._set_headers()

        data = load_session()
        data.update({
            "region":       region,
            "project_id":   project_id,
            "agency_id":    agency_id,
            "workspace_id": workspace_id,
            "cftk":         cftk,
            "cookies":      {c.name: c.value for c in self.http.cookies},
            "cookie_str":   cookie_str,
            "saved_at":     time.time(),
        })
        save_session(data)

    def restore(self):
        d = load_session()
        if not d: return False
        self.region       = d.get("region", "cn-north-9")
        self.project_id   = d.get("project_id")
        self.agency_id    = d.get("agency_id", "")
        self.workspace_id = d.get("workspace_id", "")
        self.cftk         = d.get("cftk")
        for k, v in d.get("cookies", {}).items():
            self.http.cookies.set(k, v)
        self._set_headers()
        return bool(self.project_id)

    def _set_headers(self):
        h = {
            "accept":           "application/json, text/plain, */*",
            "content-type":     "application/json; charset=UTF-8",
            "region":           self.region,
            "projectname":      self.region,
            "agencyid":         self.agency_id,
            "x-language":       "zh-cn",
            "x-requested-with": "XMLHttpRequest",
            "x-target-services":"modelarts-iam5",
            "origin":           "https://console.huaweicloud.com",
            "user-agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "referer":          f"https://console.huaweicloud.com/modelarts/?locale=zh-cn"
                                f"&agencyId={self.agency_id}&region={self.region}",
        }
        if self.cftk:
            h["cftk"]     = self.cftk
            h["cf2-cftk"] = "cftk"
        self.http.headers.update(h)

    def check_login(self) -> bool:
        """调用 /modelarts/rest/me 验证当前 session 是否仍然有效。
        返回 True 表示有效，False 表示已过期或未登录。
        """
        try:
            r = self.http.get(
                "https://console.huaweicloud.com/modelarts/rest/me",
                headers={
                    "accept":           "application/json, text/plain, */*",
                    "region":           self.region or "",
                    "projectname":      self.region or "",
                    "agencyid":         self.agency_id or "",
                    "x-language":       "zh-cn",
                    "x-requested-with": "XMLHttpRequest",
                    "x-target-services":"modelarts-iam5",
                    "cftk":             self.cftk or "",
                    "cf2-cftk":         "cftk",
                },
                timeout=10,
            )
            if r.status_code != 200:
                return False
            data = r.json()
            # projectId 存在且非空说明 session 有效
            return bool(data.get("projectId"))
        except Exception:
            return False

    @property
    def base(self):
        return f"{CONSOLE_BASE}/modelarts/rest/trainingJob/v2/{self.project_id}"

    def _checked_request(self, method: str, url: str, **kwargs):
        """执行 HTTP 请求，遇到网络/SSL 异常时先检查登录状态：
        - session 已过期 → 抛 SessionExpiredError（exit 2，触发自动重登录）
        - 纯网络问题    → 抛原始异常
        """
        try:
            return getattr(self.http, method)(url, **kwargs)
        except requests.exceptions.SSLError as e:
            if not self.check_login():
                raise SessionExpiredError("session expired, please login again") from e
            raise
        except requests.exceptions.ConnectionError as e:
            if not self.check_login():
                raise SessionExpiredError("session expired, please login again") from e
            raise

    def get(self, path, **params):
        return self._checked_request(
            "get", f"{self.base}{path}",
            params=params or None, timeout=20,
        )

    def post(self, path, body):
        return self._checked_request(
            "post", f"{self.base}{path}",
            json=body, timeout=30,
        )

# ── API ──────────────────────────────────────────────────────

class API:
    def __init__(self, sess: ConsoleSession):
        self.sess = sess

    def _safe_json(self, r):
        """安全解析响应 JSON。
        若解析失败（如响应体为空），自动检查登录状态：
        - 登录已过期 → 抛出 SessionExpiredError
        - 其他原因   → 抛出原始 JSONDecodeError
        """
        try:
            return r.json()
        except requests.exceptions.JSONDecodeError as e:
            # 响应体为空或非 JSON，检查登录状态
            if not self.sess.check_login():
                raise SessionExpiredError(
                    "登录凭据已过期，请重新执行 macli login"
                ) from e
            raise

    def list_jobs(self, limit=50, offset=0) -> dict:
        r = self.sess.post("/training-job-searches", {
            "workspace_id": self.sess.workspace_id,
            "limit":        limit,
            "offset":       offset,
            "order":        "desc",
            "sort_by":      "create_time",
        })
        dprint(f"[dim]API list_jobs offset={offset} limit={limit} → {r.status_code}[/dim]")
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]列表失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job(self, job_id: str) -> dict:
        r = self.sess.get(f"/training-jobs/{job_id}")
        dprint(f"[dim]API get_job {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]详情失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job_events(self, job_id: str, limit: int = 50, offset: int = 0,
                       start_time=None, end_time=None, order: str = "desc",
                       pattern: str = "", level: str = "") -> dict:
        params = {
            "limit": limit,
            "offset": offset,
            "order": order,
            "pattern": pattern,
            "level": level,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        r = self.sess.get(f"/training-jobs/{job_id}/events", **params)
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]事件查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job_tasks(self, job_id: str) -> list:
        r = self.sess.get(f"/training-jobs/{job_id}/tasks")
        dprint(f"[dim]API get_job_tasks {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code == 200:
            data = self._safe_json(r)
            tasks = data if isinstance(data, list) else []
            dprint(f"[dim]  tasks: {[t.get('name','?') for t in tasks]}[/dim]")
            return tasks
        cprint(f"[red]任务列表查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return []

    def get_job_log_url(self, job_id: str, task_id: str, content_type: str = "application/octet-stream") -> dict:
        url = f"{self.sess.base}/training-jobs/{job_id}/tasks/{task_id}/logs/url"
        r = self.sess.http.get(url, params={"Content-Type": content_type}, timeout=30)
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]日志下载链接获取失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def download_from_obs_url(self, obs_url: str, timeout: int = 120):
        http = _new_session()
        return http.get(
            obs_url,
            headers={
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "origin": "https://console.huaweicloud.com",
                "referer": "https://console.huaweicloud.com/",
                "user-agent": self.sess.http.headers.get("user-agent", "Mozilla/5.0"),
            },
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )

    def query_usage_range(self, query: str, start: int, end: int, step: int = 60) -> dict:
        url = (
            f"https://console.huaweicloud.com/modelarts/rest/api/aompod/v1/"
            f"{self.sess.project_id}/aom/api/v1/query_range"
        )
        payload = {"query": query, "start": start, "end": end, "step": step}
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://console.huaweicloud.com",
            "referer": "https://console.huaweicloud.com/",
            "user-agent": "Mozilla/5.0",
            "region": self.sess.region,
            "projectname": self.sess.region,
            "agencyid": self.sess.agency_id,
            "cftk": self.sess.cftk,
            "cf2-cftk": "cftk",
        }
        http = _new_session()
        for c in self.sess.http.cookies:
            http.cookies.set(c.name, c.value)
        r = http.post(url, params=payload, json=payload, headers=headers, timeout=30)
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]usage 查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_exec_status(self, job_id: str) -> dict:
        r = self.sess.get(f"/training-jobs/{job_id}/exec/status")
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]CloudShell 状态查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    # flavor_id 对应卡数
    FLAVOR_MAP = {
        1: "modelarts.pool.visual.xlarge",
        2: "modelarts.pool.visual.2xlarge",
        4: "modelarts.pool.visual.4xlarge",
        8: "modelarts.pool.visual.8xlarge",
    }

    def copy_job(self, job_id: str,
                 new_gpu_count: int = None,
                 new_name: str = None,
                 description: str = None,
                 command: str = None) -> dict:
        src = self.get_job(job_id)
        if not src:
            return {}

        body = copy.deepcopy(src)

        # 清理只读字段
        meta = body.get("metadata", {})
        for k in ("id", "uuid", "create_time", "update_time",
                  "training_experiment_reference", "tags"):
            meta.pop(k, None)

        # 作业名称
        if new_name:
            meta["name"] = new_name
        else:
            clean = re.sub(r'-copy-\d+$', '', meta.get("name", "job"))
            meta["name"] = f"{clean}-copy-{int(time.time()) % 100000}"

        # 描述
        if description is not None:
            meta["description"] = description

        body["metadata"] = meta

        # 清理服务端只读字段
        body.pop("status", None)
        body.pop("ftjob_config", None)
        # endpoints 中只保留 key_pair_names（密钥对配置），清掉 task_urls（运行时动态填充）
        endpoints = body.get("endpoints", {})
        ssh = endpoints.get("ssh", {})
        if ssh:
            endpoints["ssh"] = {"key_pair_names": ssh.get("key_pair_names", [])}
            body["endpoints"] = endpoints
        else:
            body.pop("endpoints", None)
        res = body.get("spec", {}).get("resource", {})
        res.pop("pool_info", None)
        res.pop("main_container_allocated_resources", None)

        # 修改规格（只改 flavor_id）
        if new_gpu_count is not None:
            flavor = self.FLAVOR_MAP.get(new_gpu_count)
            if not flavor:
                cprint(f"[red]不支持的卡数 {new_gpu_count}，可选: 1/2/4/8[/red]")
                return {}
            body["spec"]["resource"]["flavor_id"] = flavor

        # 修改启动命令
        if command is not None:
            body.setdefault("algorithm", {})["command"] = command

        r = self.sess.post("/training-jobs", body)
        dprint(f"[dim]API copy_job → {r.status_code}[/dim]")
        if r.status_code in (200, 201):
            created = self._safe_json(r)
            dprint(f"[dim]  新作业 ID={created.get('metadata',{}).get('id','?')} phase={created.get('status',{}).get('phase','?')}[/dim]")
            return created
        dprint(f"[dim]  error body: {r.text[:400]}[/dim]")
        cprint(f"[red]创建失败 {r.status_code}: {r.text[:400]}[/red]")
        return {}

    def delete_job(self, job_id: str) -> bool:
        r = self.sess.http.delete(
            f"{self.sess.base}/training-jobs/{job_id}",
            json={}, timeout=15)
        dprint(f"[dim]API delete_job {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code in (200, 202):
            return True
        cprint(f"[red]删除失败 {r.status_code}: {r.text[:200]}[/red]")
        return False

    def stop_job(self, job_id: str) -> bool:
        r = self.sess.post(
            f"/training-jobs/{job_id}/actions",
            {"action_type": "terminate"})
        dprint(f"[dim]API stop_job {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code in (200, 202):
            return True
        cprint(f"[red]终止失败 {r.status_code}: {r.text[:200]}[/red]")
        return False

    def get_ssh(self, job: dict) -> list:
        """从详情中提取 SSH 信息，路径: endpoints.ssh.task_urls[].url，并补充 port。"""
        try:
            return enrich_ssh_entries(job.get("endpoints", {}).get("ssh", {}).get("task_urls", []))
        except Exception:
            return []

# ── 命令 ─────────────────────────────────────────────────────

def _sess_or_exit():
    sess = ConsoleSession()
    if not sess.restore():
        cprint("[red]未找到 session，请先执行 login[/red]")
        sys.exit(1)
    return sess

STATUS_COLOR = {
    "Running": "green", "Pending": "yellow", "Waiting": "yellow",
    "Failed": "red", "Completed": "blue", "Stopped": "dim",
}

def _me(http, region, cftk, agency_id="") -> dict:
    """调用 /modelarts/rest/me 获取当前 region 的 projectId"""
    r = http.get(
        "https://console.huaweicloud.com/modelarts/rest/me",
        headers={
            "accept":           "application/json, text/plain, */*",
            "region":           region,
            "projectname":      region,
            "agencyid":         agency_id,
            "x-language":       "zh-cn",
            "x-requested-with": "XMLHttpRequest",
            "x-target-services":"modelarts-iam5",
            "cftk":             cftk,
            "cf2-cftk":         "cftk",
            "user-agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "referer":          f"https://console.huaweicloud.com/modelarts/?locale=zh-cn"
                                f"&agencyId={agency_id}&region={region}",
        },
        timeout=15)
    if r.status_code == 200 and r.text.strip():
        try:
            return r.json()
        except Exception:
            return {}
    return {}


_ME_PROBE_REGIONS = [
    "cn-north-4", "cn-north-9", "cn-east-3", "cn-south-1",
    "cn-east-4", "cn-southwest-2", "ap-southeast-3",
]


def _me_probe(http, cftk) -> dict:
    """尝试多个常见 region，返回第一个包含 supportRegions 的 me 响应。"""
    for region in _ME_PROBE_REGIONS:
        me = _me(http, region, cftk)
        if me.get("supportRegions"):
            return me
    return {}


def _fetch_workspaces(sess) -> list:
    """调用 workspaces 接口，返回 [{"id":..., "name":...}, ...]"""
    url = (f"https://console.huaweicloud.com/modelarts/rest/v1"
           f"/{sess.project_id}/workspaces"
           f"?offset=0&limit=100&sort_by=update_time&order=desc&name=&filter_accessible=true")
    try:
        r = sess.http.get(url, timeout=15)
        if r.status_code == 200:
            return r.json().get("workspaces", [])
    except Exception as e:
        cprint(f"[yellow]获取工作空间失败: {e}[/yellow]")
    return []


def _select_workspace(sess) -> str:
    """展示工作空间列表，让用户选择，返回选中的 workspace_id"""
    dprint("[cyan]获取工作空间列表...[/cyan]")
    workspaces = _fetch_workspaces(sess)

    if not workspaces:
        cprint("[yellow]未获取到工作空间，请手动输入 Workspace ID[/yellow]")
        return input("Workspace ID: ").strip()

    cprint("\n[bold]可用工作空间：[/bold]")
    for i, ws in enumerate(workspaces, 1):
        status = "" if ws.get("status") == "NORMAL" else f" [{ws.get('status')}]"
        cprint(f"  [cyan]{i}.[/cyan] {ws['name']}{status}")

    while True:
        choice = input(f"\n请选择工作空间 (1-{len(workspaces)}): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(workspaces):
            chosen = workspaces[int(choice) - 1]
            cprint(f"[green]✓ 已选择：{chosen['name']}[/green]")
            return chosen["id"]
        cprint("[red]输入无效，请重试[/red]")



def _http_login(domain: str, username: str, password: str,
                service: str = "https://console.huaweicloud.com/console/",
                otp_provider=None):
    """
    纯 HTTP 登录华为云（IAM 用户 + 短信 MFA），返回 (cookie_str, session)。
    失败返回 ("", None)。
    """
    import urllib.parse, json as _json

    UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

    def q(s):
        return urllib.parse.quote(s, safe="")

    s = _new_session()
    s.headers.update({"User-Agent": UA})

    login_page = (
        f"https://auth.huaweicloud.com/authui/login.html"
        f"?locale=zh-cn&service={q(service)}#/login"
    )
    base_headers = {"User-Agent": UA, "Referer": login_page}

    # Step 1: 打开登录页，获取初始 cookie
    dprint("[dim][1] 打开登录页...[/dim]")
    s.get(login_page, timeout=15)

    # Step 2: login/verify
    dprint("[dim][2] 验证账号...[/dim]")
    s.post(
        "https://auth.huaweicloud.com/authui/login/verify",
        headers=base_headers,
        data={"userName": username, "userType": "name", "accountName": q(domain)},
        timeout=15,
    )

    # Step 3: 密码登录
    dprint("[dim][3] 提交密码...[/dim]")
    payload = {
        "userpasswordcredentials.domain":       q(domain),
        "userpasswordcredentials.domainType":   "name",
        "userpasswordcredentials.username":     q(username),
        "userpasswordcredentials.userInfoType": "name",
        "userpasswordcredentials.countryCode":  "",
        "userpasswordcredentials.verifycode":   "",
        "userpasswordcredentials.password":     q(password),
        "userpasswordcredentials.riskMonitorJson":
            _json.dumps({"devID": "", "hwMeta": ""}, separators=(",", ":")),
        "__checkbox_warnCheck": "true",
        "isAjax":  "true",
        "Submit":  "Login",
    }
    r = s.post(
        "https://auth.huaweicloud.com/authui/validateUser.action",
        headers=base_headers,
        params={"locale": "zh-cn", "service": service},
        data=payload,
        timeout=15,
    )
    try:
        data = r.json()
    except Exception:
        cprint(f"[red]密码登录响应解析失败: {r.text[:200]}[/red]")
        return "", None
    if data.get("loginResult") != "success":
        cprint(f"[red]密码登录失败: {data.get('loginResult')} — {data.get('loginMessage','')}[/red]")
        return "", None
    dprint("[dim]密码验证通过[/dim]")

    # Step 4: 获取 MFA 信息（含 IAMCSRF）
    dprint("[dim][4] 获取 MFA 信息...[/dim]")
    r = s.get(
        "https://auth.huaweicloud.com/authui/getAntiPhishingInfo",
        headers=base_headers,
        params={"isSupport": "false"},
        timeout=15,
    )
    try:
        anti = r.json()
    except Exception:
        cprint(f"[red]MFA 信息获取失败: {r.text[:200]}[/red]")
        return "", None
    if anti.get("result") != "success":
        cprint(f"[red]MFA 信息获取失败: {anti}[/red]")
        return "", None
    iamcsrf = anti.get("IAMCSRF", "")
    dprint(f"[dim]MFA 响应字段: {list(anti.keys())}[/dim]")

    # Step 5: 发送短信验证码
    mfa_referer = (
        "https://auth.huaweicloud.com/authui/loginVerification.html"
        f"?service={q(service)}"
    )
    mfa_headers = {"User-Agent": UA, "Referer": mfa_referer}

    # 手机号字段名因账号类型不同而异，遍历常见字段
    phone = (anti.get("phoneNum") or anti.get("phone") or
             anti.get("mobilePhone") or anti.get("mobile") or
             anti.get("verifyPhone") or "")
    if phone:
        cprint(f"[yellow]验证码将发送至: {phone}[/yellow]")
    else:
        cprint("[yellow]验证码将发送至您绑定的手机[/yellow]")

    if otp_provider is None:
        input("按 Enter 发送验证码...")
    dprint("[dim][5] 发送短信...[/dim]")
    r = s.post(
        "https://auth.huaweicloud.com/authui/sendLoginSms",
        headers=mfa_headers,
        timeout=15,
    )
    try:
        sms_resp = r.json()
    except Exception:
        sms_resp = {}
    if sms_resp.get("result") not in {"success", "faster"}:
        cprint(f"[yellow]短信发送响应: {sms_resp}（继续输入验证码）[/yellow]")
    else:
        cprint("[green]✓ 验证码已发送[/green]")

    # Step 6: 输入并提交验证码
    if otp_provider is not None:
        dprint("[dim][6] 调用 otp_provider 获取验证码...[/dim]")
        sms_code = otp_provider()
    else:
        sys.stdout.flush()
        sms_code = input("\n请输入收到的 6 位验证码: ").strip()
    if not sms_code:
        cprint("[red]验证码不能为空[/red]")
        return "", None

    dprint("[dim][6] 提交验证码...[/dim]")
    r = s.post(
        "https://auth.huaweicloud.com/authui/validateUser",
        headers=mfa_headers,
        params={"locale": "zh-cn", "service": service},
        data={"smsCode": sms_code, "step": "afterAntiPhishing", "IAMCSRF": iamcsrf},
        allow_redirects=False,
        timeout=15,
    )
    if not r.is_redirect:
        cprint(f"[red]验证码提交后未跳转 (HTTP {r.status_code}): {r.text[:200]}[/red]")
        return "", None
    location = r.headers.get("location", "")
    if "actionErrors=419" in location:
        cprint("[red]验证码无效或已失效（419），请重试[/red]")
        return "", None

    next_url = urllib.parse.urljoin(r.url, location)
    dprint(f"[dim][7] 跟随跳转: {next_url[:80]}...[/dim]")

    # Step 7: 跟随重定向链，收集完整 cookie
    for _ in range(10):
        r = s.get(next_url, allow_redirects=False, timeout=15)
        if r.is_redirect and r.headers.get("location"):
            next_url = urllib.parse.urljoin(r.url, r.headers["location"])
        else:
            break

    # 拼出 cookie 字符串（按优先级排序）
    COOKIE_ORDER = [
        "SSOTGC", "SSOJTC", "SID", "J_SESSION_ID", "J_SESSION_REGION",
        "cftk", "console_cftk", "agencyID", "domain_tag", "user_tag",
        "usite", "auth_cdn", "Site", "HWWAFSESID", "HWWAFSESTIME",
        "third-party-access", "x-framework-ob", "xyt_nmk",
    ]
    all_cookies = {c.name: c.value for c in s.cookies}
    ordered = [f"{k}={all_cookies[k]}" for k in COOKIE_ORDER if k in all_cookies]
    remaining = [f"{k}={v}" for k, v in all_cookies.items() if k not in COOKIE_ORDER]
    ck = "; ".join(ordered + remaining)

    if not ck:
        cprint("[yellow]未获取到任何 cookie[/yellow]")
        return "", None

    dprint(f"[dim]获取到 {len(all_cookies)} 个 cookie，共 {len(ck)} 字符[/dim]")
    return ck, s


def _extract_cftk(cookie_str: str) -> str:
    """从 cookie 字符串中提取 cftk 值"""
    for part in cookie_str.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == "cftk":
            return v.strip()
    return ""


def _get_cookie_from_args_or_input(args):
    """按优先级获取 cookie：--cookie 参数 > session 缓存 > 已保存凭据 > 交互登录
    返回 (cookie_str, http_session_or_none)"""
    # 1. --cookie 参数直接给了
    if getattr(args, "cookie", None):
        ck = args.cookie.strip()
        dprint(f"[green]✓ 使用 --cookie 参数（{len(ck)} 字符）[/green]")
        return ck, None

    # 2. 从已保存的 session 中读取 cookie
    d = load_session()
    ck = d.get("cookie_str", "")
    if ck:
        dprint(f"[green]✓ 从 session 读取 cookie（{len(ck)} 字符）[/green]")
        return ck, None

    # 3. 从已保存的账号密码自动登录
    saved = _load_saved_creds()
    if saved.get("domain") and saved.get("username") and saved.get("password"):
        cprint(f"[cyan]使用已保存的账号自动登录：[bold]{saved['username']}[/bold] @ {saved['domain']}[/cyan]")
        ck, http_s = _http_login(saved["domain"], saved["username"], saved["password"])
        if ck:
            dprint("[green]✓ 自动登录成功[/green]")
            return ck, http_s
        cprint("[yellow]已保存的账号登录失败（密码可能已变更），请重新输入[/yellow]")
        _clear_saved_creds()

    # 4. 交互式 HTTP 登录
    import getpass as _getpass
    cprint("[bold cyan]══ 华为云 IAM 登录 ══[/bold cyan]")
    _domain   = input("租户名/原华为云账号: ").strip()
    _username = input("IAM 用户名/邮件地址: ").strip()
    _password = _getpass.getpass("IAM 用户密码: ")

    if not all([_domain, _username, _password]):
        cprint("[red]账号信息不完整[/red]")
        return "", None

    ck, http_s = _http_login(_domain, _username, _password)
    if not ck:
        return "", None

    if _save_creds(_domain, _username, _password):
        cprint("[green]✓ 账号密码已安全保存，下次自动登录[/green]")
    else:
        dprint("[dim]密码未保存（keyring 不可用）[/dim]")

    return ck, http_s


def _setup_session_from_cookie(ck: str, interactive: bool, http_session=None) -> None:
    """用 cookie 初始化 session，interactive=True 时交互选择 region/workspace。
    http_session: 来自 _http_login 的原始 session，有完整的 console 上下文；
                  为 None 时从 cookie 字符串重建（适用于 --cookie 粘贴路径）。
    """
    cftk = _extract_cftk(ck)
    dprint("[green]✓ 从 cookie 中提取 cftk[/green]")

    # 先保存 cookie，登录凭证不依赖后续探测
    d = load_session(); d["cookie_str"] = ck; save_session(d)

    # 复用登录 session（有 console 上下文），或从 cookie 字符串重建
    if http_session is not None:
        http = http_session
    else:
        http = _new_session()
        for part in ck.split(";"):
            k, _, v = part.strip().partition("=")
            if k: http.cookies.set(k.strip(), v.strip())

    # 探测 region/project_id
    dprint("[cyan]获取账号区域信息...[/cyan]")
    me = _me_probe(http, cftk)
    support_regions = me.get("supportRegions", [])

    if not support_regions:
        # _me 探测失败：cookie 已保存，提示后续手动配置区域
        cprint("[yellow]无法自动获取区域信息，请登录后执行：[/yellow]")
        cprint("[dim]  macli region select    # 配置区域[/dim]")
        cprint("[dim]  macli workspace select  # 配置工作空间[/dim]")
        return

    if interactive:
        regions = sorted(r for r in support_regions if r in REGION_NAMES)
        cprint("\n[bold]可用区域：[/bold]")
        for i, r in enumerate(regions, 1):
            cprint(f"  [cyan]{i}.[/cyan] {r}  [dim]{REGION_NAMES[r]}[/dim]")
        while True:
            choice = input(f"\n请选择区域 (1-{len(regions)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(regions):
                region = regions[int(choice) - 1]; break
            cprint("[red]输入无效，请重试[/red]")
    else:
        region = me.get("region", "cn-north-9")
        dprint(f"[cyan]使用默认区域: {region}  {REGION_NAMES.get(region,'')}[/cyan]")

    # 获取该 region 的 project_id
    dprint(f"[cyan]获取 {region} 的 project_id...[/cyan]")
    me2        = _me(http, region, cftk)
    project_id = me2.get("projectId", "")
    agency_id  = me2.get("id") or me2.get("userId", "")
    if not project_id:
        cprint(f"[red]无法获取 {region} 的 project_id[/red]"); sys.exit(1)
    dprint(f"[green]✓ region={region}  project_id={project_id}[/green]")

    sess = ConsoleSession()
    sess.init(ck, region, project_id, agency_id, cftk, "")

    if interactive:
        wsid = _select_workspace(sess)
        sess.workspace_id = wsid
        d = load_session(); d["workspace_id"] = wsid; save_session(d)
    else:
        dprint("[cyan]获取工作空间列表...[/cyan]")
        workspaces = _fetch_workspaces(sess)
        if workspaces:
            wsid  = workspaces[0]["id"]
            wsname = workspaces[0]["name"]
            sess.workspace_id = wsid
            d = load_session(); d["workspace_id"] = wsid; save_session(d)
            cprint(f"[green]✓ 默认工作空间: {wsname}[/green]")
            dprint("[dim]提示：使用 workspace select 切换工作空间[/dim]")
        else:
            cprint("[yellow]未获取到工作空间，使用 workspace select 手动设置[/yellow]")


def _manual_cookie_input() -> str:
    """展示手动获取 cookie 指南，让用户粘贴，返回 cookie 字符串"""
    cprint("""
[bold cyan]══ 手动获取 cookie ══[/bold cyan]
[yellow]1.[/yellow] 浏览器访问 https://auth.huaweicloud.com/authui/login.html
[yellow]2.[/yellow] 完成登录（含短信验证码）
[yellow]3.[/yellow] 登录后访问 https://console.huaweicloud.com/modelarts/
[yellow]4.[/yellow] F12 → Network → 找到 [green]training-job-searches[/green] 的 POST 请求
[yellow]5.[/yellow] 复制 Request Headers 中的 [green]cookie[/green] 整行，粘贴后回车
""")
    cprint("[yellow]请粘贴 cookie：[/yellow]")
    sys.stdout.flush()
    return sys.stdin.readline().strip()


def cmd_login(args):
    # 登录命令始终重新认证，不复用缓存 cookie（避免直接返回过期的旧 cookie）
    if not getattr(args, "cookie", None):
        d = load_session()
        d.pop("cookie_str", None)
        save_session(d)

    ck, http_session = _get_cookie_from_args_or_input(args)

    # 登录失败时，只有 --cookie 路径才继续
    if not ck:
        if not getattr(args, "cookie", None):
            sys.exit(1)
        # --cookie 传了但为空，展示指南让用户粘贴
        ck = _manual_cookie_input()
        http_session = None
        if not ck:
            sys.exit(1)

    # 验证 cftk，失败时展示指南让用户重新粘贴
    if not _extract_cftk(ck):
        cprint("[red]cookie 无效（无法提取 cftk），请重新获取[/red]")
        ck = _manual_cookie_input()
        http_session = None
        if not ck:
            sys.exit(1)
        if not _extract_cftk(ck):
            cprint("[red]cookie 仍然无效，退出[/red]")
            sys.exit(1)

    _setup_session_from_cookie(ck, interactive=getattr(args, "interactive", False),
                               http_session=http_session)
    cprint(f"\n[green]✓ 登录成功！[/green]")


def cmd_workspace_list(args):
    sess = _sess_or_exit()
    workspaces = _fetch_workspaces(sess)
    if not workspaces:
        cprint("[yellow]未获取到工作空间[/yellow]"); return

    current = sess.workspace_id
    if getattr(args, "json", False):
        _json_out([{"id": ws["id"], "name": ws["name"],
                    "status": ws.get("status", ""),
                    "description": ws.get("description", ""),
                    "current": ws["id"] == current} for ws in workspaces])
        return

    t = Table(title="工作空间列表", header_style="bold cyan")
    t.add_column("",     width=2)
    t.add_column("名称", style="green")
    t.add_column("状态", width=8)
    t.add_column("描述")
    for ws in workspaces:
        marker = "[bold yellow]*[/bold yellow]" if ws["id"] == current else " "
        sc = "green" if ws.get("status") == "NORMAL" else "yellow"
        t.add_row(marker, ws["name"],
                  f"[{sc}]{ws.get('status','')}[/{sc}]",
                  ws.get("description") or "")
    console.print(t)
    cprint("[dim]* 当前选中[/dim]")
def cmd_workspace_select(args):
    sess = _sess_or_exit()
    workspaces = _fetch_workspaces(sess)
    if not workspaces:
        cprint("[yellow]未获取到工作空间[/yellow]"); return

    wsid = None
    if args.id:
        for ws in workspaces:
            if ws["id"] == args.id:
                wsid = ws["id"]; chosen_name = ws["name"]; break
        if not wsid:
            cprint(f"[red]未找到 ID={args.id}[/red]"); sys.exit(1)
    elif args.name:
        for ws in workspaces:
            if ws["name"] == args.name:
                wsid = ws["id"]; chosen_name = ws["name"]; break
        if not wsid:
            cprint(f"[red]未找到名称 '{args.name}'[/red]"); sys.exit(1)
    else:
        wsid = _select_workspace(sess)
        chosen_name = wsid

    sess.workspace_id = wsid
    d = load_session(); d["workspace_id"] = wsid; save_session(d)
    if args.id or args.name:
        cprint(f"[green]✓ 已切换到：{chosen_name}[/green]")


def cmd_region_list(args):
    """列出 cookie 中记录的 region，以及当前选中的"""
    d = load_session()
    if not d:
        cprint("[red]未登录[/red]"); return
    current = d.get("region", "")
    # 重新从 me 接口拿最新列表
    sess = ConsoleSession(); sess.restore()
    me = _me(sess.http, current, sess.cftk, sess.agency_id)
    regions = sorted(r for r in me.get("supportRegions", [current]) if r in REGION_NAMES or r == current)
    if getattr(args, "json", False):
        _json_out([{"id": r, "name_cn": REGION_NAMES.get(r, ""),
                    "current": r == current} for r in regions])
        return

    t = Table(title="可用区域", header_style="bold cyan")
    t.add_column("",       width=2)
    t.add_column("区域",   style="green", no_wrap=True)
    t.add_column("中文名", style="dim")
    for r in regions:
        marker = "[bold yellow]*[/bold yellow]" if r == current else " "
        t.add_row(marker, r, REGION_NAMES.get(r, ""))
    console.print(t)
    cprint("[dim]* 当前选中[/dim]")
    
def cmd_region_select(args):
    """切换区域，自动更新 project_id，并重新选择工作空间"""
    d = load_session()
    if not d:
        cprint("[red]未登录[/red]"); return

    sess = ConsoleSession(); sess.restore()
    me = _me(sess.http, sess.region, sess.cftk, sess.agency_id)
    regions = me.get("supportRegions", [])

    # 确定目标 region
    if args.name:
        if args.name not in regions:
            cprint(f"[red]'{args.name}' 不在可用区域列表中[/red]"); sys.exit(1)
        region = args.name
    else:
        if not regions:
            cprint("[red]无法获取区域列表[/red]"); sys.exit(1)
        regions = sorted(r for r in regions if r in REGION_NAMES)
        cprint("\n[bold]可用区域：[/bold]")
        for i, r in enumerate(regions, 1):
            label = f"{r}  [dim]{REGION_NAMES[r]}[/dim]"
            cprint(f"  [cyan]{i}.[/cyan] {label}")
        while True:
            choice = input(f"\n请选择区域 (1-{len(regions)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(regions):
                region = regions[int(choice) - 1]; break
            cprint("[red]输入无效，请重试[/red]")

    # 获取新 project_id
    dprint(f"[cyan]获取 {region} 的 project_id...[/cyan]")
    me2 = _me(sess.http, region, sess.cftk, sess.agency_id)
    project_id = me2.get("projectId", "")
    if not project_id:
        cprint(f"[red]无法获取 {region} 的 project_id[/red]"); sys.exit(1)
    dprint(f"[green]✓ region={region}  project_id={project_id}[/green]")

    # 更新 session
    sess.region = region
    sess.project_id = project_id
    sess._set_headers()
    me2_agency = me2.get("id") or me2.get("userId", "")
    d = load_session()
    d["region"]     = region
    d["project_id"] = project_id
    d["agency_id"]  = me2_agency
    d["workspace_id"] = ""
    save_session(d)

    # 自动选第一个工作空间
    dprint("[cyan]获取工作空间列表...[/cyan]")
    workspaces = _fetch_workspaces(sess)
    if workspaces:
        wsid = workspaces[0]["id"]
        wsname = workspaces[0]["name"]
        sess.workspace_id = wsid
        d = load_session(); d["workspace_id"] = wsid; save_session(d)
        cprint(f"[green]✓ 已切换到 {region}，工作空间: {wsname}[/green]")
        dprint("[dim]提示：使用 workspace select 切换工作空间[/dim]")
    else:
        cprint(f"[green]✓ 已切换到 {region}[/green]")
        cprint("[yellow]未获取到工作空间，使用 workspace select 手动设置[/yellow]")


def cmd_logout(args):
    """清除已保存的登录凭据（保留持久化偏好配置）"""
    session_keys = {
        "region", "project_id", "agency_id", "workspace_id", "cftk",
        "cookies", "cookie_str", "saved_at", "ssh_port_cache",
    }

    data = load_session()
    had_session_state = any(k in data for k in session_keys)
    for k in session_keys:
        data.pop(k, None)

    p = _config_path()
    if data:
        save_session(data)
    elif p.exists():
        p.unlink()

    purge = getattr(args, "purge", False)
    cleared_creds = _clear_saved_creds() if purge else False

    if had_session_state or cleared_creds:
        parts = []
        if had_session_state: parts.append("登录 session")
        if cleared_creds:     parts.append("Keychain 账号密码")
        cprint(f"[green]✓ 已清除：{' 及 '.join(parts)}[/green]")
        if data:
            cprint("[dim]已保留其他配置：如 autologin、identityfiles、exec backend 等[/dim]")
        if had_session_state and not purge:
            cprint("[dim]提示：keyring 账号密码已保留（autologin 可复用），"
                   "如需彻底清除请使用 macli logout --purge[/dim]")
    else:
        cprint("[yellow]当前没有已保存的登录凭据[/yellow]")


# ── Watch（定时检查任务）─────────────────────────────────────
_WATCH_KEY         = "watch"
_WATCH_PLIST_LABEL = "com.macli.watch"
_WATCH_PLIST_PATH  = Path.home() / "Library" / "LaunchAgents" / f"{_WATCH_PLIST_LABEL}.plist"
_WATCH_STATE_FILE  = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "watch_state.json"
_WATCH_CRON_MARKER = "# macli-watch"
_IS_LINUX          = sys.platform.startswith("linux")


def _load_watch_cfg() -> dict:
    return load_session().get(_WATCH_KEY, {})


def _save_watch_cfg(cfg: dict):
    data = load_session()
    data[_WATCH_KEY] = cfg
    save_session(data)


def _watch_plist_xml(interval_secs: int, script_path: str,
                     threshold_hours: int, log_path: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"'
        ' "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        '<plist version="1.0">\n'
        '<dict>\n'
        f'    <key>Label</key>\n    <string>{_WATCH_PLIST_LABEL}</string>\n'
        '    <key>ProgramArguments</key>\n    <array>\n'
        f'        <string>{sys.executable}</string>\n'
        f'        <string>{script_path}</string>\n'
        f'        <string>--threshold-hours</string>\n'
        f'        <string>{threshold_hours}</string>\n'
        '    </array>\n'
        f'    <key>StartInterval</key>\n    <integer>{interval_secs}</integer>\n'
        '    <key>RunAtLoad</key>\n    <false/>\n'
        f'    <key>StandardOutPath</key>\n    <string>{log_path}</string>\n'
        f'    <key>StandardErrorPath</key>\n    <string>{log_path}</string>\n'
        '</dict>\n'
        '</plist>\n'
    )


def _launchctl(action: str) -> bool:
    r = _subprocess.run(
        ["launchctl", action, "-w", str(_WATCH_PLIST_PATH)],
        capture_output=True,
    )
    dprint(f"[dim]launchctl {action} → {r.returncode}[/dim]")
    return r.returncode == 0


def _launchctl_is_loaded() -> bool:
    r = _subprocess.run(
        ["launchctl", "list", _WATCH_PLIST_LABEL],
        capture_output=True,
    )
    return r.returncode == 0


def _cron_get_lines() -> list:
    r = _subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if r.returncode != 0:
        return []
    return [l for l in r.stdout.splitlines() if l]


def _cron_set_lines(lines: list):
    text = "\n".join(lines) + "\n"
    _subprocess.run(["crontab", "-"], input=text, text=True, check=True)


def _cron_watch_is_active() -> bool:
    return any(_WATCH_CRON_MARKER in l for l in _cron_get_lines())


def _cron_watch_install(interval_h: float, script_path: str,
                        threshold_hours: int, log_path: str):
    lines = [l for l in _cron_get_lines() if _WATCH_CRON_MARKER not in l]
    if interval_h >= 1 and interval_h == int(interval_h):
        cron_expr = f"0 */{int(interval_h)} * * *"
    else:
        mins = max(1, int(interval_h * 60))
        cron_expr = f"*/{mins} * * * *"
    entry = (f"{cron_expr} {sys.executable} {script_path}"
             f" --threshold-hours {threshold_hours} >> {log_path} 2>&1"
             f" {_WATCH_CRON_MARKER}")
    lines.append(entry)
    _cron_set_lines(lines)


def _cron_watch_remove():
    lines = [l for l in _cron_get_lines() if _WATCH_CRON_MARKER not in l]
    _cron_set_lines(lines)


def cmd_watch(args):
    action = getattr(args, "watch_action", "status")
    if action == "enable":
        _watch_enable(args)
    elif action == "disable":
        _watch_disable()
    elif action == "run":
        _watch_run(args)
    else:
        _watch_status()


def _watch_status():
    cfg    = _load_watch_cfg()
    if _IS_LINUX:
        active = _cron_watch_is_active()
        if cfg.get("enabled") and active:
            cprint("[green]watch：[bold]已启用（cron 运行中）[/bold][/green]")
        elif cfg.get("enabled") and not active:
            cprint("[yellow]watch：已配置但 cron 条目未找到（建议重新 enable）[/yellow]")
        else:
            cprint("[dim]watch：未启用[/dim]")
    else:
        loaded = _launchctl_is_loaded()
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


def _watch_enable(args):
    script_arg      = getattr(args, "script",          None)
    interval_h      = getattr(args, "interval",        1)
    threshold_hours = getattr(args, "threshold_hours", 72)

    # 找脚本路径：参数 > 已保存配置 > 包内默认路径
    _bundled = Path(__file__).resolve().parents[2] / "scripts" / "check_jobs.py"
    if script_arg:
        script_path = Path(script_arg).expanduser().resolve()
    else:
        stored = _load_watch_cfg().get("script_path", "")
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
        if _cron_watch_is_active():
            cprint("[dim]watch 已在运行，重新安装 cron 条目...[/dim]")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        _cron_watch_install(interval_h, str(script_path), threshold_hours, log_path)
        _save_watch_cfg(cfg)
        cprint(f"[green]✓ watch 已启用，每 {interval_h}h 执行一次（cron）[/green]")
        cprint(f"  脚本：{script_path}")
        cprint(f"  日志：{log_path}")
    else:
        if _launchctl_is_loaded():
            cprint("[dim]watch 已在运行，重新加载...[/dim]")
        # 写 plist
        _WATCH_PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _launchctl("unload")   # 先卸载（忽略失败）
        _WATCH_PLIST_PATH.write_text(
            _watch_plist_xml(interval_s, str(script_path), threshold_hours, log_path),
            encoding="utf-8",
        )

        if _launchctl("load"):
            _save_watch_cfg(cfg)
            cprint(f"[green]✓ watch 已启用，每 {interval_h}h 执行一次[/green]")
            cprint(f"  脚本：{script_path}")
            cprint(f"  日志：{log_path}")
            cprint(f"  plist：{_WATCH_PLIST_PATH}")
        else:
            cprint("[red]launchctl load 失败[/red]")
            cprint(f"  plist：{_WATCH_PLIST_PATH}")
            sys.exit(1)


def _watch_disable():
    if _IS_LINUX:
        _cron_watch_remove()
        cprint("[green]✓ watch 已停用，cron 条目已移除[/green]")
    else:
        _launchctl("unload")
        if _WATCH_PLIST_PATH.exists():
            _WATCH_PLIST_PATH.unlink()
        cprint("[green]✓ watch 已停用，launchd 任务已卸载[/green]")

    cfg = _load_watch_cfg()
    cfg["enabled"] = False
    _save_watch_cfg(cfg)


def _watch_run(args):
    """立即执行一次检查脚本（用于测试）。"""
    cfg            = _load_watch_cfg()
    script_arg     = getattr(args, "script",          None)
    threshold_hours = getattr(args, "threshold_hours", None)

    script_path = Path(script_arg).expanduser() if script_arg else Path(cfg.get("script_path", ""))
    if not script_path.exists():
        cprint("[red]未找到检查脚本，请先 macli watch enable --script PATH 或用 --script 指定[/red]")
        sys.exit(1)

    if threshold_hours is None:
        threshold_hours = cfg.get("threshold_hours", 72)

    cprint(f"[cyan]立即运行：{script_path}[/cyan]")
    result = _subprocess.run(
        [sys.executable, str(script_path), "--threshold-hours", str(threshold_hours)],
        text=True,
    )
    sys.exit(result.returncode)


# ── macli server ─────────────────────────────────────────────
_SERVER_KEY         = "server"
_SERVER_PLIST_LABEL = "com.macli.server"
_SERVER_PLIST_PATH  = Path.home() / "Library" / "LaunchAgents" / f"{_SERVER_PLIST_LABEL}.plist"
_SERVER_LOG_FILE    = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "server.log"
_SERVER_PID_FILE    = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "server.pid"
_MACLI_LOG_FILE     = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "macli.log"


def _load_server_cfg() -> dict:
    return load_session().get(_SERVER_KEY, {})

def _save_server_cfg(cfg: dict):
    data = load_session()
    data[_SERVER_KEY] = cfg
    save_session(data)

def _server_plist_xml(port: int) -> str:
    log = str(_SERVER_LOG_FILE)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"'
        ' "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        '<plist version="1.0">\n<dict>\n'
        f'    <key>Label</key>\n    <string>{_SERVER_PLIST_LABEL}</string>\n'
        '    <key>ProgramArguments</key>\n    <array>\n'
        f'        <string>{sys.executable}</string>\n'
        '        <string>-m</string>\n        <string>macli</string>\n'
        '        <string>server</string>\n        <string>run</string>\n'
        f'        <string>--port</string>\n        <string>{port}</string>\n'
        '    </array>\n'
        '    <key>RunAtLoad</key>\n    <true/>\n'
        '    <key>KeepAlive</key>\n    <true/>\n'
        f'    <key>StandardOutPath</key>\n    <string>{log}</string>\n'
        f'    <key>StandardErrorPath</key>\n    <string>{log}</string>\n'
        '</dict>\n</plist>\n'
    )

def _server_launchctl(action: str) -> bool:
    r = _subprocess.run(
        ["launchctl", action, "-w", str(_SERVER_PLIST_PATH)],
        capture_output=True, text=True,
    )
    return r.returncode == 0

def _server_launchctl_is_loaded() -> bool:
    r = _subprocess.run(
        ["launchctl", "list", _SERVER_PLIST_LABEL],
        capture_output=True, text=True,
    )
    return r.returncode == 0


def _server_linux_is_running() -> bool:
    if not _SERVER_PID_FILE.exists():
        return False
    try:
        pid = int(_SERVER_PID_FILE.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


def _server_linux_start(port: int) -> None:
    """Start server as background process. Raises RuntimeError on startup failure."""
    import time as _time
    _SERVER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    _server_linux_stop()
    log_fd = open(str(_SERVER_LOG_FILE), "a")
    proc = _subprocess.Popen(
        [sys.executable, "-m", "macli", "server", "run", "--port", str(port)],
        stdout=log_fd,
        stderr=log_fd,
        start_new_session=True,
        close_fds=True,
    )
    log_fd.close()
    _time.sleep(1.5)
    if proc.poll() is not None:
        try:
            lines = _SERVER_LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = "\n".join(lines[-5:])
        except Exception:
            tail = "(无法读取日志)"
        raise RuntimeError(f"server 启动失败：\n{tail}")
    _SERVER_PID_FILE.write_text(str(proc.pid), encoding="utf-8")


def _server_linux_stop():
    if not _SERVER_PID_FILE.exists():
        return
    try:
        pid = int(_SERVER_PID_FILE.read_text(encoding="utf-8").strip())
        import signal as _signal
        os.kill(pid, _signal.SIGTERM)
    except (ValueError, ProcessLookupError, PermissionError):
        pass
    finally:
        _SERVER_PID_FILE.unlink(missing_ok=True)


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

def _server_status():
    cfg    = _load_server_cfg()
    port   = cfg.get("port", 8086)
    if _IS_LINUX:
        running = _server_linux_is_running()
        if cfg.get("enabled") and running:
            cprint("[green]server：已启用（后台进程运行中）[/green]")
        elif cfg.get("enabled"):
            cprint("[yellow]server：已配置但进程未运行（执行 macli server enable 重新启动）[/yellow]")
        else:
            cprint("[dim]server：未启用[/dim]")
    else:
        loaded = _server_launchctl_is_loaded()
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

def _server_enable(args):
    port = getattr(args, "port", None) or _load_server_cfg().get("port", 8086)
    cfg = _load_server_cfg()
    cfg.update({"enabled": True, "port": port})
    _save_server_cfg(cfg)
    if _IS_LINUX:
        if _server_linux_is_running():
            cprint("[dim]server 已在运行，重新启动...[/dim]")
        try:
            _server_linux_start(port)
        except RuntimeError as e:
            cprint(f"[red]{e}[/red]")
            sys.exit(1)
        cprint(f"[green]✓ server 已启用  http://localhost:{port}/gpu[/green]")
        cprint(f"  日志：{_SERVER_LOG_FILE}")
    else:
        if _server_launchctl_is_loaded():
            cprint("[dim]server 已在运行，重新加载...[/dim]")
        _SERVER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SERVER_PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _server_launchctl("unload")
        _SERVER_PLIST_PATH.write_text(_server_plist_xml(port), encoding="utf-8")
        ok = _server_launchctl("load")
        if ok:
            cprint(f"[green]✓ server 已启用  http://localhost:{port}/gpu[/green]")
        else:
            cprint(f"[yellow]⚠ 配置已写入，launchctl load 返回非零（可能已在运行）[/yellow]")
            cprint(f"  http://localhost:{port}/gpu")

def _server_disable():
    if _IS_LINUX:
        _server_linux_stop()
    else:
        _server_launchctl("unload")
        if _SERVER_PLIST_PATH.exists():
            _SERVER_PLIST_PATH.unlink()
    cfg = _load_server_cfg()
    cfg["enabled"] = False
    _save_server_cfg(cfg)
    cprint("[green]✓ server 已停用[/green]")

def _server_run(args):
    """在当前进程内启动 FastAPI server（阻塞）。由 launchd 或手动调用。"""
    import threading as _threading
    from io import StringIO as _StringIO

    port = getattr(args, "port", None) or _load_server_cfg().get("port", 8086)

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

    _RATE_LIMIT = 10.0
    _cache_lock = _threading.Lock()
    _cache      = {"last_run": 0.0, "ansi": "", "plain": "", "jobs": []}

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

    def _render_jobs(jobs: list, use_ansi: bool) -> str:
        if not jobs:
            return "No running jobs.\n"

        # 按创建时间升序累计 GPU 数，最先占满配额的任务为稳定主机
        _GPU_QUOTA = 8
        sorted_asc = sorted(jobs, key=lambda r: r.get("create_time") or 0)
        stable_ids: set = set()
        _used = 0
        for r in sorted_asc:
            n = len(r.get("gpu_devices") or []) or 1
            if _used + n <= _GPU_QUOTA:
                stable_ids.add(r.get("job_id"))
            _used += n

        buf = _StringIO()
        con = _RConsole(file=buf, force_terminal=use_ansi, force_jupyter=False,
                        highlight=False, markup=False, width=140,
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
        for r in jobs:
            devs      = r.get("gpu_devices", [])
            job_id    = r.get("job_id") or "?"
            job_short = job_id[:8]
            flag      = "🏠" if job_id in stable_ids else "🔴"
            ssh       = r.get("ssh_port") or "—"
            cpu       = _fmt_pct(r.get("cpu"))
            mem       = _fmt_mem(r.get("mem"))
            created   = _fmt_created(r.get("create_time"))
            dur       = _fmt_dur(r.get("duration_ms"))
            if not devs:
                tbl.add_row(flag, job_short, ssh, cpu, mem, created, dur, "—")
            else:
                for i, d in enumerate(devs):
                    cell = _dev_cell(d, use_ansi)
                    if i == 0:
                        tbl.add_row(flag, job_short, ssh, cpu, mem, created, dur, cell)
                    else:
                        tbl.add_row("", "", "", "", "", "", "", cell)
        con.print(tbl)
        return buf.getvalue()

    def _refresh():
        result = _subprocess.run(
            [sys.executable, "-m", "macli", "usage", "--probe", "--json"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            try:
                data = json.loads(result.stdout)
                jobs = data.get("jobs", [])
                ansi  = _render_jobs(jobs, True)
                plain = _render_jobs(jobs, False)
            except (json.JSONDecodeError, KeyError) as e:
                ansi = plain = f"parse error: {e}\n{result.stdout[:300]}\n"
        else:
            err  = (result.stdout or result.stderr or "").strip()[:400]
            ansi = plain = f"Error (exit {result.returncode}):\n{err}\n"
        with _cache_lock:
            _cache["ansi"]     = ansi
            _cache["plain"]    = plain
            if result.returncode == 0:
                _cache["jobs"] = jobs
            _cache["last_run"] = time.monotonic()

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
        _GPU_QUOTA = 8
        sorted_asc = sorted(jobs, key=lambda r: r.get("create_time") or 0)
        stable_ids: set = set()
        _used = 0
        for r in sorted_asc:
            n = len(r.get("gpu_devices") or []) or 1
            if _used + n <= _GPU_QUOTA:
                stable_ids.add(r.get("job_id"))
            _used += n
        out = []
        for r in jobs:
            devices = []
            for d in (r.get("gpu_devices") or []):
                util      = d.get("util")
                vram_used = d.get("vram_used_mb") or 0
                vram_tot  = d.get("vram_total_mb") or 1
                idle = ((util or 0) * 100 == 0 and vram_used / vram_tot * 100 < 3)
                devices.append({**d, "idle": idle})
            out.append({**r,
                        "preemptible": r.get("job_id") not in stable_ids,
                        "gpu_devices": devices})
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

    @app.get("/health")
    def health():
        def _fetch():
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
                "running": _server_linux_is_running() if _IS_LINUX else _server_launchctl_is_loaded(),
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
                "running":         _cron_watch_is_active() if _IS_LINUX else _launchctl_is_loaded(),
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
            }

            # ── exec / identityfiles ──────────────────────────
            idf_map, idf_default = load_identityfiles()
            exec_info = {
                "backend":             sess.get("exec_backend", "cloudshell"),
                "identityfiles":       idf_map,
                "default_identityfile": idf_default,
            }

            return {
                "login":    login,
                "server":   server,
                "watch":    watch,
                "autologin": autologin,
                "exec":     exec_info,
            }

        data, _ = _health_cache.get(_fetch)
        with _cache_lock:
            last = _cache["last_run"]
        gpu_age = round(time.monotonic() - last, 1) if last > 0 else None
        return {"status": "ok", "port": port, "gpu_cache_age_s": gpu_age, **data}

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
        # 按创建时间累计 GPU 数，标记稳定/临时
        _GPU_QUOTA = 8
        sorted_asc = sorted(data, key=lambda r: r.get("create_time") or 0)
        stable_ids: set = set()
        _used = 0
        for r in sorted_asc:
            n = r.get("gpu_count") or 1
            if _used + n <= _GPU_QUOTA:
                stable_ids.add(r.get("id"))
            _used += n
        enriched = [{**r, "preemptible": r.get("id") not in stable_ids} for r in data]
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


def cmd_whoami(args):
    d = load_session()
    if not d: cprint("[red]未登录[/red]"); return
    ck = d.get("cookies", {})
    age = (time.time() - d.get("saved_at", 0)) / 3600

    if getattr(args, "json", False):
        _json_out({
            "user":         ck.get("masked_user", ""),
            "domain":       ck.get("masked_domain", ""),
            "region":       d.get("region", ""),
            "project_id":   d.get("project_id", ""),
            "agency_id":    d.get("agency_id", ""),
            "workspace_id": d.get("workspace_id", ""),
            "saved_at":     d.get("saved_at"),
            "session_age_hours": round(age, 1),
        })
        return

    console.print(Panel(
        f"[bold]用户:[/bold]         {ck.get('masked_user','?')}\n"
        f"[bold]租户:[/bold]         {ck.get('masked_domain','?')}\n"
        f"[bold]Region:[/bold]       {d.get('region','?')}\n"
        f"[bold]Project ID:[/bold]   {d.get('project_id','?')}\n"
        f"[bold]User ID:[/bold]      {d.get('agency_id','?')}\n"
        f"[bold]Workspace ID:[/bold] {d.get('workspace_id','?')}\n"
        f"[bold]保存于:[/bold]       "
        f"{datetime.fromtimestamp(d.get('saved_at',0), tz=_CST).strftime('%Y-%m-%d %H:%M')}"
        f"  ({age:.1f}h 前)",
        title="Session 状态", border_style="green"))
    
def _resolve_jobs_ssh_map(api: "API", jobs: list, refresh: bool = False) -> dict:
    """为一组 jobs 解析 SSH entries，并复用 Running 作业的端口缓存。"""
    port_cache = PortCache() if refresh else PortCache().load()
    if refresh:
        dprint("[dim]--refresh: 已清空端口缓存，强制重新拉取[/dim]")

    running_ids = {j.get("metadata", {}).get("id", "") for j in jobs
                   if j.get("status", {}).get("phase") == "Running"
                   and j.get("metadata", {}).get("id")}
    stale = port_cache.evict_non_running(running_ids)
    if stale:
        dprint(f"[dim]清理 {len(stale)} 条非 Running 端口缓存[/dim]")

    ssh_map: dict = {}
    hit_count = fetched_count = 0
    for j in jobs:
        job_id = j.get("metadata", {}).get("id", "")
        phase  = j.get("status", {}).get("phase", "")
        if not job_id:
            continue
        before = port_cache.get(job_id)
        ssh_map[job_id] = resolve_ssh(api, job_id, phase, port_cache)
        if before is not None:
            hit_count += 1
        elif phase == "Running":
            fetched_count += 1
    port_cache.save()
    dprint(f"[dim]端口缓存：命中 {hit_count} 条，新拉取 {fetched_count} 条[/dim]")
    return ssh_map



_STATUS_ALIAS: dict = {
    "running":    {"Running"},
    "failed":     {"Failed"},
    "terminated": {"Stopped", "Terminated"},
    "pending":    {"Pending", "Waiting"},
}


def _apply_job_filters(jobs: list, args) -> list:
    """根据 args 中的过滤条件对作业列表进行本地过滤，返回过滤后的列表。"""
    if getattr(args, "recent", None):
        try:
            delta_ms = parse_recent(args.recent)
        except ValueError as e:
            cprint(f"[red]{e}[/red]"); sys.exit(1)
        cutoff = int(time.time() * 1000) - delta_ms
        jobs = [j for j in jobs
                if (j.get("metadata", {}).get("create_time") or 0) >= cutoff]

    status_filter: set = set()
    if getattr(args, "running",    False): status_filter |= _STATUS_ALIAS["running"]
    if getattr(args, "failed",     False): status_filter |= _STATUS_ALIAS["failed"]
    if getattr(args, "terminated", False): status_filter |= _STATUS_ALIAS["terminated"]
    if getattr(args, "pending",    False): status_filter |= _STATUS_ALIAS["pending"]
    for s in (getattr(args, "status", None) or []):
        status_filter |= _STATUS_ALIAS.get(s.lower(), {s})
    if status_filter:
        jobs = [j for j in jobs
                if j.get("status", {}).get("phase", "") in status_filter]

    if getattr(args, "gpu_count", None):
        allowed = set(args.gpu_count)
        jobs = [j for j in jobs
                if j.get("spec", {}).get("resource", {})
                    .get("pool_info", {}).get("accelerator_num") in allowed]

    if getattr(args, "name", None):
        jobs = [j for j in jobs
                if j.get("metadata", {}).get("name", "") == args.name]

    limit = getattr(args, "limit", None)
    if limit:
        jobs = jobs[:limit]
    dprint(f"[dim]_apply_job_filters: 过滤后剩余 {len(jobs)} 个作业[/dim]")
    return jobs


def _read_piped_ids() -> list:
    """从 stdin 读取作业 ID（每行一个）。若 stdin 是终端则返回空列表。"""
    if sys.stdin.isatty():
        return []
    return [ln.strip() for ln in sys.stdin if ln.strip()]


def _fetch_all_jobs(api: "API", max_items: int = 500) -> list:
    """分页拉取所有作业，最多拉取 max_items 条（API 每页限制 50）。"""
    PAGE = 50
    jobs: list = []
    offset = 0
    while True:
        data   = api.list_jobs(limit=PAGE, offset=offset)
        total  = data.get("total", 0)
        page   = data.get("items", [])
        jobs  += page
        offset += len(page)
        if not page or offset >= total or offset >= max_items:
            break
    dprint(f"[dim]_fetch_all_jobs: 共拉取 {len(jobs)} 个作业（total={total}）[/dim]")
    return jobs


def cmd_list_jobs(args):
    sess = _sess_or_exit()
    api  = API(sess)

    # 所有过滤在本地完成，分页拉取（每页最多 50，API 硬限制）
    need_filter = bool(args.recent or args.running or args.failed
                       or args.terminated or args.pending or args.gpu_count
                       or args.name or args.status)
    if need_filter:
        jobs = _fetch_all_jobs(api)
    else:
        PAGE = 50
        jobs  = []
        total = 0
        offset = 0
        while True:
            data   = api.list_jobs(limit=PAGE, offset=offset)
            total  = data.get("total", 0)
            page   = data.get("items", [])
            jobs  += page
            offset += len(page)
            if len(jobs) >= args.limit or not page or offset >= total:
                break

    jobs = _apply_job_filters(jobs, args)

    # ── count 模式：只输出数量 ──
    if getattr(args, "action", None) == "count":
        if getattr(args, "json", False):
            _json_out({"count": len(jobs)})
        else:
            print(len(jobs))
        return

    # ── SSH 端口解析（PortCache）─────────────────────────────
    ssh_map = _resolve_jobs_ssh_map(api, jobs, refresh=getattr(args, "refresh", False))

    if getattr(args, "json", False):
        out = []
        for j in jobs:
            job_id = j.get("metadata", {}).get("id", "")
            out.append(job_to_dict(j, ssh_override=ssh_map.get(job_id, [])))
        _json_out(out)
        return

    if not jobs:
        cprint("[yellow]没有找到符合条件的训练作业[/yellow]"); return

    total = data.get("total", 0)
    t = Table(title=f"训练作业（总计 {total} 个，过滤后显示 {len(jobs)} 个）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#",        width=3)
    t.add_column("名称",     style="green", no_wrap=True, max_width=20)
    t.add_column("ID",       style="dim",   no_wrap=True, width=40)
    t.add_column("状态",     no_wrap=True,  width=13)
    t.add_column("时长",     width=10)
    t.add_column("卡数",     width=4)
    t.add_column("SSH端口",  width=12)
    t.add_column("创建时间", width=17)
    t.add_column("创建者",   width=10)

    for i, j in enumerate(jobs, 1):
        meta    = j.get("metadata", {})
        st      = j.get("status",   {})
        spec    = j.get("spec",     {})
        phase   = st.get("phase", "?")
        color   = STATUS_COLOR.get(phase, "white")
        gpu_num = spec.get("resource", {}).get("pool_info", {}).get("accelerator_num", "?")
        job_id  = meta.get("id", "")
        t.add_row(
            str(i), meta.get("name", ""), job_id,
            f"[{color}]{phase}[/{color}]",
            ms_to_hms(st.get("duration")), f"{gpu_num}卡",
            ssh_ports_summary(ssh_map.get(job_id, [])),
            ts_to_str(meta.get("create_time")), meta.get("user_name", ""),
        )
    console.print(t)
    if getattr(args, "refresh", False):
        cprint("[dim]已清空缓存并重新拉取所有端口信息（--refresh）[/dim]")
    else:
        cprint("[dim]端口缓存：Running 作业端口已自动缓存，--refresh 可强制重新拉取[/dim]")


def cmd_query(args):
    """按条件筛选作业，输出 ID（可管道给其他命令）。"""
    sess = _sess_or_exit()
    api  = API(sess)

    jobs = _apply_job_filters(_fetch_all_jobs(api), args)

    if getattr(args, "json", False):
        _json_out([{
            "id":     j.get("metadata", {}).get("id", ""),
            "name":   j.get("metadata", {}).get("name", ""),
            "status": j.get("status", {}).get("phase", ""),
        } for j in jobs])
        return

    # 管道模式：只输出 ID，每行一个，干净地传给下游命令
    if not sys.stdout.isatty():
        for j in jobs:
            print(j.get("metadata", {}).get("id", ""))
        return

    # 终端模式：显示预览表格（不解析 SSH 端口，速度快）
    if not jobs:
        cprint("[yellow]没有找到符合条件的训练作业[/yellow]")
        return

    t = Table(title=f"查询结果（共 {len(jobs)} 个作业）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#",        width=3)
    t.add_column("名称",     style="green", no_wrap=True, max_width=24)
    t.add_column("ID",       style="dim",   no_wrap=True, width=40)
    t.add_column("状态",     no_wrap=True,  width=13)
    t.add_column("时长",     width=10)
    t.add_column("卡数",     width=4)
    t.add_column("创建时间", width=17)

    for i, j in enumerate(jobs, 1):
        meta    = j.get("metadata", {})
        st      = j.get("status",   {})
        spec    = j.get("spec",     {})
        phase   = st.get("phase", "?")
        color   = STATUS_COLOR.get(phase, "white")
        gpu_num = spec.get("resource", {}).get("pool_info", {}).get("accelerator_num", "?")
        t.add_row(
            str(i), meta.get("name", ""), meta.get("id", ""),
            f"[{color}]{phase}[/{color}]",
            ms_to_hms(st.get("duration")), f"{gpu_num}卡",
            ts_to_str(meta.get("create_time")),
        )
    console.print(t)
    cprint("[dim]提示：将此命令管道给其他操作，例如：macli query [filters] | macli stop --yes[/dim]")


def cmd_ports(args):
    """列出当前 Running 作业的 SSH 端口信息。"""
    sess = _sess_or_exit()
    api  = API(sess)

    jobs = [j for j in _fetch_all_jobs(api)
            if j.get("status", {}).get("phase", "") == "Running"]
    ssh_map = _resolve_jobs_ssh_map(api, jobs, refresh=getattr(args, "refresh", False))

    if getattr(args, "json", False):
        out = []
        for j in jobs:
            meta    = j.get("metadata", {})
            res     = j.get("spec", {}).get("resource", {})
            job_id  = meta.get("id", "")
            ssh     = ssh_map.get(job_id, [])
            out.append({
                "id":          job_id,
                "name":        meta.get("name", ""),
                "status":      "Running",
                "create_time": meta.get("create_time"),
                "gpu_count":   res.get("pool_info", {}).get("accelerator_num") or 1,
                "ports":       ssh_ports_list(ssh),
                "ssh":         ssh,
            })
        _json_out(out)
        return

    if not jobs:
        cprint("[yellow]当前没有 Running 状态的训练作业[/yellow]")
        return

    t = Table(title=f"Running 作业 SSH 端口（共 {len(jobs)} 个）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#", width=3)
    t.add_column("名称", style="green", no_wrap=True, max_width=24)
    t.add_column("ID", style="dim", no_wrap=True, width=40)
    t.add_column("SSH端口", width=18)

    for i, j in enumerate(jobs, 1):
        meta = j.get("metadata", {})
        job_id = meta.get("id", "")
        t.add_row(
            str(i),
            meta.get("name", ""),
            job_id,
            ssh_ports_summary(ssh_map.get(job_id, [])),
        )
    console.print(t)
    if getattr(args, "refresh", False):
        cprint("[dim]已清空缓存并重新拉取所有 Running 作业的端口信息（--refresh）[/dim]")
    else:
        cprint("[dim]端口缓存：Running 作业端口已自动缓存，--refresh 可强制重新拉取[/dim]")

def cmd_detail(args):
    sess = _sess_or_exit()
    api  = API(sess)

    if getattr(args, "src_name", None):
        data = api.list_jobs(limit=50)
        matched = [j for j in data.get("items", [])
                   if j.get("metadata", {}).get("name", "") == args.src_name]
        if not matched:
            cprint(f"[red]未找到名称为 '{args.src_name}' 的作业[/red]")
            sys.exit(1)
        if len(matched) > 1:
            cprint(f"[yellow]找到 {len(matched)} 个同名作业，使用最新的一个[/yellow]")
        job = matched[0]
        args.job_id = job.get("metadata", {}).get("id", "")
        # 获取完整详情（列表返回的字段可能不完整）
        job = api.get_job(args.job_id)
        if not job: sys.exit(1)
    else:
        job = api.get_job(args.job_id)
        if not job: sys.exit(1)

    if getattr(args, "json", False):
        _json_out(job_to_dict(job))
        return

    meta  = job.get("metadata", {})
    st    = job.get("status",   {})
    spec  = job.get("spec",     {})
    phase = st.get("phase", "?")
    color = STATUS_COLOR.get(phase, "white")

    console.print(Panel(
        f"[bold]名称:[/bold]   {meta.get('name','')}\n"
        f"[bold]状态:[/bold]   [{color}]{phase}[/{color}]\n"
        f"[bold]时长:[/bold]   {ms_to_hms(st.get('duration'))}\n"
        f"[bold]规格(目标):[/bold] {_fmt_flavor(spec.get('resource',{}))}\n"
        f"[bold]规格(实际):[/bold] {_fmt_actual(spec.get('resource',{}))}\n"
        f"[bold]镜像:[/bold]   {job.get('algorithm',{}).get('engine',{}).get('image_url','')}\n"
        f"[bold]创建者:[/bold] {meta.get('user_name','')}   "
        f"{ts_to_str(meta.get('create_time'))}",
        title=f"作业详情  {args.job_id}", border_style="cyan"))

    ssh_list = api.get_ssh(job)
    if ssh_list:
        cprint("\n[bold green]🔑 SSH 连接[/bold green]")
        for s in ssh_list:
            url = s["url"]
            port = s.get("port")
            port_show = port if port is not None else "—"
            cprint(f"  {s['task']} [dim](port: {port_show})[/dim]: {url}")
            if url.startswith("ssh://"):
                inner = url[6:]
                user, _, hp = inner.partition("@")
                host, _, port = hp.partition(":")
                cprint(f"  [bold cyan]ssh -p {port} -i ~/.ssh/KeyPair-liusonghua.pem {user}@{host}[/bold cyan]")
    else:
        cprint("\n[yellow]该作业暂无 SSH 信息（未运行或不是调试模式）[/yellow]")


def cmd_events(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job = api.get_job(args.job_id)
    if not job:
        sys.exit(1)
    create_time = (job.get("metadata", {}) or {}).get("create_time")
    end_time = int(time.time() * 1000)

    data = api.get_job_events(
        args.job_id,
        limit=args.limit,
        offset=args.offset,
        start_time=create_time,
        end_time=end_time,
        order="desc",
        pattern="",
        level="",
    )
    if not data:
        sys.exit(1)

    if getattr(args, "json", False):
        _json_out(data)
        return

    events = data.get("events", [])
    total = data.get("total", len(events))
    limit = data.get("limit", args.limit)
    offset = data.get("offset", args.offset)
    start_time = data.get("start_time", "")
    end_time = data.get("end_time", "")

    title = f"作业事件  {args.job_id}"
    if start_time or end_time:
        title += f"\n[dim]{start_time} ~ {end_time}[/dim]"
    t = Table(title=title, header_style="bold cyan", show_lines=False)
    t.add_column("#", width=4)
    t.add_column("时间", style="green", width=25, no_wrap=True)
    t.add_column("级别", width=10, no_wrap=True)
    t.add_column("来源", width=10, no_wrap=True)
    t.add_column("消息", overflow="fold")

    level_color = {
        "Info": "blue",
        "Warning": "yellow",
        "Error": "red",
        "Fatal": "bold red",
    }
    for i, ev in enumerate(events, start=offset + 1):
        level = ev.get("level", "")
        color = level_color.get(level, "white")
        t.add_row(
            str(i),
            ev.get("time", ""),
            f"[{color}]{level}[/{color}]" if level else "",
            ev.get("source", ""),
            ev.get("message", ""),
        )
    console.print(t)
    cprint(f"[dim]显示 {len(events)} / {total} 条事件（limit={limit}, offset={offset}）[/dim]")


def _pick_log_task(tasks: list, preferred: str = None, interactive: bool = False) -> str:
    if preferred:
        return preferred
    if not tasks:
        return "worker-0"
    if len(tasks) == 1:
        return tasks[0].get("task") or "worker-0"
    if not interactive:
        chosen = tasks[0]
        task = chosen.get("task") or "worker-0"
        ip   = chosen.get("ip", "")
        dprint(f"[dim]自动选择任务：{task} {ip}（共 {len(tasks)} 个节点，可用 --task 指定）[/dim]")
        return task
    cprint("[bold]可用任务：[/bold]")
    for i, item in enumerate(tasks, 1):
        task = item.get("task", "")
        ip = item.get("ip", "")
        host_ip = item.get("host_ip", "")
        cprint(f"  [cyan]{i}.[/cyan] {task} [dim]{ip} / {host_ip}[/dim]")
    while True:
        choice = input(f"\n请选择任务 (1-{len(tasks)}): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(tasks):
            return tasks[int(choice) - 1].get("task") or "worker-0"
        cprint("[red]输入无效，请重试[/red]")


def _save_response_with_progress(resp, outpath: Path) -> int:
    total = int(resp.headers.get("content-length") or 0)
    size = 0
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with outpath.open("wb") as f:
        if total > 0:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("下载日志", total=total)
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    f.write(chunk)
                    n = len(chunk)
                    size += n
                    progress.update(task, advance=n)
        else:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("下载日志", total=None)
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    f.write(chunk)
                    n = len(chunk)
                    size += n
                    progress.update(task, advance=n)
    return size


def cmd_log(args):
    sess = _sess_or_exit()
    api  = API(sess)

    if not args.output:
        cprint("[red]请显式指定 --output <OUTPUT_PATH>[/red]")
        sys.exit(1)

    tasks = api.get_job_tasks(args.job_id)
    task_name = _pick_log_task(tasks, preferred=args.task)

    if getattr(args, "json", False):
        _json_out({
            "job_id": args.job_id,
            "task": task_name,
            "tasks": tasks,
        })
        return

    info = api.get_job_log_url(args.job_id, task_name)
    obs_url = info.get("obs_url", "")
    if not obs_url:
        cprint("[red]未获取到日志下载地址[/red]")
        sys.exit(1)

    outpath = Path(args.output).expanduser()
    if outpath.is_dir():
        outpath = outpath / f"{args.job_id[:8]}_{task_name}.log"
    resp = api.download_from_obs_url(obs_url, timeout=args.timeout)
    if resp.status_code != 200:
        cprint(f"[red]日志下载失败 {resp.status_code}[/red]")
        sys.exit(1)
    size = _save_response_with_progress(resp, outpath)

    cprint(f"[green]✓ 日志已导出[/green] {outpath} [dim]({size} bytes)[/dim]")
    cprint(f"[dim]job={args.job_id}  task={task_name}[/dim]")


def _build_usage_query(metric_name: str, job_id: str, window_ms: int = 59999) -> str:
    return (
        f'avg(label_replace(avg_over_time({metric_name}'
        f'{{service_id="{job_id}",container_name="modelarts-training"}}'
        f'[{window_ms}ms]),"__name__","{metric_name}","",""))by(__name__,service_id)'
    )


def _usage_series_stats(values: list) -> dict:
    pairs = []
    for item in values or []:
        try:
            ts = int(item[0])
            val = float(item[1])
            pairs.append((ts, val))
        except Exception:
            continue
    if not pairs:
        return {"count": 0, "latest": None, "avg": None, "max": None}
    only_vals = [v for _, v in pairs]
    return {
        "count": len(pairs),
        "latest": pairs[-1][1],
        "avg": sum(only_vals) / len(only_vals),
        "max": max(only_vals),
        "start": pairs[0][0],
        "end": pairs[-1][0],
        "values": pairs,
    }


def _fmt_usage_value(metric_key: str, val):
    if val is None:
        return "--"
    if metric_key in {"cpu_util", "memory_util", "gpu_util", "gpu_mem_util"}:
        return f"{val * 100:.2f}%"
    if metric_key == "cpu_used_core":
        return f"{val:.4f} cores"
    if metric_key in {"memory_used_megabytes", "gpu_mem_used_megabytes"}:
        return f"{val:.2f} MB"
    return str(val)


def _sparkline(values: list, width: int = 32) -> str:
    ticks = "▁▂▃▄▅▆▇█"
    vals = []
    for item in values or []:
        try:
            vals.append(float(item[1] if isinstance(item, (list, tuple)) and len(item) >= 2 else item))
        except Exception:
            continue
    if not vals:
        return "·" * width
    if len(vals) > width:
        step = len(vals) / width
        sampled = []
        for i in range(width):
            idx = min(int(i * step), len(vals) - 1)
            sampled.append(vals[idx])
        vals = sampled
    elif len(vals) < width:
        vals = [vals[0]] * (width - len(vals)) + vals
    vmin, vmax = min(vals), max(vals)
    if vmax - vmin < 1e-12:
        level = 0 if vmax <= 0 else len(ticks) // 2
        return ticks[level] * len(vals)
    chars = []
    for v in vals:
        idx = int(round((v - vmin) / (vmax - vmin) * (len(ticks) - 1)))
        idx = max(0, min(len(ticks) - 1, idx))
        chars.append(ticks[idx])
    return "".join(chars)


_USAGE_METRIC_ALIASES = {
    "cpu":  "cpu",
    "mem":  "mem",  "memory": "mem",
    "gpu":  "gpu",
    "vram": "vram", "gpu_mem": "vram", "gmem": "vram",
}

def _parse_metrics_filter(raw: list) -> set:
    """将用户传入的 metric 名列表规范化为内部 key 集合（cpu/mem/gpu/vram）。"""
    result = set()
    for item in raw or []:
        key = _USAGE_METRIC_ALIASES.get(item.lower())
        if key:
            result.add(key)
    return result or {"cpu", "mem", "gpu", "vram"}   # 空 = 全部


def _util_bar(util: float, width: int = 36) -> str:
    """探针模式用：单采样点实心进度条（无时序数据，不用 sparkline）。"""
    filled = round(max(0.0, min(1.0, util)) * width)
    return "█" * filled + "░" * (width - filled)


def _usage_panel_text(result: dict, filter_set: set = None) -> str:
    m        = result["metrics"]
    is_probe = result.get("probe", False)
    # 每卡详情（probe 模式专有）
    gpu_devs  = m.get("gpu_devices", []) if is_probe else []
    want_gpu  = filter_set is None or "gpu"  in filter_set
    want_vram = filter_set is None or "vram" in filter_set
    use_per_gpu = bool(gpu_devs) and (want_gpu or want_vram)

    lines = []
    all_rows = [
        ("CPU",  "cpu",  "cpu_util",     "cpu_used_core"),
        ("内存", "mem",  "memory_util",  "memory_used_megabytes"),
        ("GPU",  "gpu",  "gpu_util",     "gpu_mem_used_megabytes"),
        ("显存", "vram", "gpu_mem_util", "gpu_mem_used_megabytes"),
    ]
    # 有每卡数据时跳过通用 GPU/VRAM 行，改用下方的 per-GPU 渲染
    rows = [(t, uk, sk) for t, k, uk, sk in all_rows
            if (filter_set is None or k in filter_set)
            and not (use_per_gpu and k in {"gpu", "vram"})]

    start_ts = None
    end_ts   = None
    for title, util_key, used_key in rows:
        util = m.get(util_key, {})
        used = m.get(used_key, {})
        if start_ts is None and util.get("start"):
            start_ts = util.get("start")
        if util.get("end"):
            end_ts = util.get("end")
        latest_util = util.get("latest")
        percent = f"{latest_util * 100:.1f}%" if latest_util is not None else "--"
        latest_used = _fmt_usage_value(used_key, used.get("latest"))
        avg_used    = _fmt_usage_value(used_key, used.get("avg"))
        if is_probe:
            bar = _util_bar(latest_util or 0)
        else:
            bar = _sparkline(util.get("values", []), width=36)
        color = 'green' if (latest_util or 0) < 0.5 else 'yellow' if (latest_util or 0) < 0.8 else 'red'
        lines.append(f"[bold]{title:<4}[/bold] [{color}]{bar}[/{color}] {percent}")
        if is_probe:
            # 单点采样，latest==avg，只显示一行值
            if used.get("latest") is not None:
                lines.append(f"      {latest_used}")
        else:
            lines.append(f"      latest={latest_used}   avg={avg_used}")

    # ── 每卡 GPU/VRAM 行（probe 模式）────────────────────────────
    for dev in gpu_devs:
        idx        = dev["index"]
        util       = dev.get("util",         0.0)
        vram_used  = dev.get("vram_used_mb",  0.0)
        vram_total = dev.get("vram_total_mb", 0.0)
        vram_util  = dev.get("vram_util",     0.0)

        color = 'green' if util < 0.5 else 'yellow' if util < 0.8 else 'red'

        if want_gpu:
            bar = _util_bar(util)
            lines.append(f"[bold]GPU{idx} [/bold][{color}]{bar}[/{color}] {util*100:.1f}%")
        if want_vram:
            vram_bar   = _util_bar(vram_util)
            vram_str   = f"{vram_used:.0f}/{vram_total:.0f} MB"
            vram_color = 'green' if vram_util < 0.5 else 'yellow' if vram_util < 0.8 else 'red'
            prefix = "     " if want_gpu else f"[bold]GPU{idx} [/bold]"
            lines.append(f"{prefix}[{vram_color}]{vram_bar}[/{vram_color}] VRAM {vram_str}  {vram_util*100:.1f}%")

    if start_ts and end_ts:
        st = datetime.fromtimestamp(start_ts, tz=_CST).strftime('%H:%M')
        ed = datetime.fromtimestamp(end_ts, tz=_CST).strftime('%H:%M')
        lines.append("")
        lines.append(f"[dim]{st} {'─' * 34} {ed}[/dim]")
    return "\n".join(lines)


def _fetch_usage_result(api: API, job_id: str, minutes: int, step: int) -> dict:
    end_ts = int(time.time())
    start_ts = end_ts - int(minutes) * 60
    metrics = {
        "cpu_used_core": "ma_container_cpu_used_core",
        "cpu_util": "ma_container_cpu_util",
        "memory_used_megabytes": "ma_container_memory_used_megabytes",
        "memory_util": "ma_container_memory_util",
        "gpu_util": "ma_container_gpu_util",
        "gpu_mem_used_megabytes": "ma_container_gpu_mem_used_megabytes",
        "gpu_mem_util": "ma_container_gpu_mem_util",
    }
    result = {
        "job_id": job_id,
        "minutes": minutes,
        "step": step,
        "metrics": {},
    }
    # 监控 API 的 util 类指标以百分比（0-100）返回，统一除以 100 归一化为 0-1
    # 以便与 probe 模式、_usage_panel_text 里的 ×100 显示逻辑保持一致
    _API_PERCENT_KEYS = {"cpu_util", "memory_util", "gpu_util", "gpu_mem_util"}

    for key, metric_name in metrics.items():
        query = _build_usage_query(metric_name, job_id)
        data = api.query_usage_range(query=query, start=start_ts, end=end_ts, step=step)
        series = (((data or {}).get("data") or {}).get("result") or [])
        values = (series[0].get("values") if series else []) or []
        stats = _usage_series_stats(values)
        if key in _API_PERCENT_KEYS:
            for field in ("latest", "avg", "max"):
                if stats[field] is not None:
                    stats[field] = stats[field] / 100
            stats["values"] = [[ts, v / 100] for ts, v in stats["values"]]
        result["metrics"][key] = stats
    return result


# ── Probe system (CloudShell-based remote metric collection) ──────────────────
#
# 每个 ProbeSpec 封装一个资源维度的探测：shell 脚本 + 输出解析。
# 所有激活的探针合并进一个脚本，通过单次 exec 连接完成采集。
# 新增平台/指标只需在 _PROBE_REGISTRY 里追加新条目即可。

def _probe_kv(text: str) -> dict:
    """解析 'key=value' 行，值转 float（失败则跳过）。"""
    result = {}
    for line in text.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            try:
                result[k.strip()] = float(v.strip())
            except ValueError:
                pass
    return result


def _probe_metric(val) -> dict:
    """将单个采样值包装成与 _usage_series_stats 兼容的 metrics 格式。"""
    if val is None:
        return {"count": 0, "latest": None, "avg": None, "max": None, "values": []}
    v = float(val)
    return {"count": 1, "latest": v, "avg": v, "max": v, "values": []}


class _ProbeSpec:
    """单个探针规格：探测哪些 filter_keys、执行什么 shell、如何解析输出。"""
    def __init__(self, key: str, filter_keys, shell: str, parse_fn):
        self.key         = key
        self.filter_keys = frozenset(filter_keys)
        self.shell       = shell.strip()
        self.parse_fn    = parse_fn   # (kv: dict) -> {metric_key: metric_dict}


# ── 各平台/指标的探针定义 ────────────────────────────────────────────────────

def _probe_parse_system(kv: dict) -> dict:
    """解析 top -bn2 输出的 CPU + 内存数据。"""
    return {
        "cpu_util":              _probe_metric(kv.get("cpu_util")),
        "cpu_used_core":         _probe_metric(None),
        "memory_util":           _probe_metric(kv.get("mem_util")),
        "memory_used_megabytes": _probe_metric(kv.get("mem_used_mb")),
    }

def _probe_parse_gpu(kv: dict) -> dict:
    gpu_count = int(kv.get("gpu_count", 0))
    devices = []
    for i in range(gpu_count):
        util       = kv.get(f"gpu_{i}_util")
        vram_used  = kv.get(f"gpu_{i}_vram_used_mb")
        vram_total = kv.get(f"gpu_{i}_vram_total_mb")
        if util is not None:
            vram_util = (vram_used / vram_total) if (vram_total and vram_total > 0) else 0.0
            devices.append({
                "index":        i,
                "util":         util,
                "vram_used_mb": vram_used  or 0.0,
                "vram_total_mb":vram_total or 0.0,
                "vram_util":    vram_util,
            })
    return {
        "gpu_util":               _probe_metric(kv.get("gpu_avg_util")),
        "gpu_mem_util":           _probe_metric(kv.get("vram_avg_util")),
        "gpu_mem_used_megabytes": _probe_metric(kv.get("vram_avg_used_mb")),
        "gpu_devices":            devices,   # list[dict], probe-only per-GPU detail
    }


_PROBE_REGISTRY: "list[_ProbeSpec]" = [
    _ProbeSpec(
        key="system",
        filter_keys={"cpu", "mem"},
        # CPU：cpuacct.usage 两次采样 delta ÷ (interval_ns × 分配核数) = 容器级利用率
        # 内存：cgroup usage_in_bytes - page cache = RSS，相对 limit 计算利用率
        # 均读自 cgroup v1，不受 host /proc 污染
        shell=r"""
_quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
_period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
_ncpu=$((_quota / _period))
_t1=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
sleep 1
_t2=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
_delta=$((_t2 - _t1))
awk -v d=$_delta -v n=$_ncpu 'BEGIN{printf "cpu_util=%.6f\n", d/(1e9*n)}'

_usage=$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes)
_limit=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
_cache=$(grep "^cache " /sys/fs/cgroup/memory/memory.stat | awk '{print $2}')
_rss=$((_usage - _cache))
awk -v u=$_rss -v l=$_limit 'BEGIN{printf "mem_used_mb=%.2f\nmem_util=%.6f\n", u/1048576, (l>0)?u/l:0}'
""",
        parse_fn=_probe_parse_system,
    ),
    _ProbeSpec(
        key="gpu",
        filter_keys={"gpu", "vram"},
        # 每卡输出 gpu_N_util / gpu_N_vram_used_mb / gpu_N_vram_total_mb
        # 同时输出聚合均值供回退使用；nvidia-smi 不可用时全零
        shell=r"""
if command -v nvidia-smi &>/dev/null; then
    nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total \
        --format=csv,noheader,nounits 2>/dev/null | \
    awk -F'[, ]+' '{
        i=$1; gu=$2; vm=$3; vt=$4
        printf "gpu_%d_util=%.6f\ngpu_%d_vram_used_mb=%.2f\ngpu_%d_vram_total_mb=%.2f\n",
               i,gu/100, i,vm, i,vt
        tgu+=gu; tvm+=vm; tvt+=vt; n++
    } END{
        if(n>0) printf "gpu_count=%d\ngpu_avg_util=%.6f\nvram_avg_used_mb=%.2f\nvram_avg_util=%.6f\n",
                        n, tgu/100/n, tvm/n, (tvt>0)?tvm/tvt:0
        else    print "gpu_count=0\ngpu_avg_util=0\nvram_avg_used_mb=0\nvram_avg_util=0"
    }'
else
    printf "gpu_count=0\ngpu_avg_util=0\nvram_avg_used_mb=0\nvram_avg_util=0\n"
fi
""",
        parse_fn=_probe_parse_gpu,
    ),
]


def _run_probes(
    sess: "ConsoleSession",
    job_id: str,
    task_name: str,
    filter_set: set,
    timeout: int = 60,
    backend: str = "cloudshell",
    ssh_entries: list = None,
    identityfile: str = None,
    ssh_opts: list = None,
) -> dict:
    """
    将 filter_set 对应的所有探针合并为一个脚本，经单次 exec 连接执行，
    返回与 _fetch_usage_result 格式兼容的 result dict。
    backend 可为 "cloudshell"（默认）或 "ssh"。
    """
    active = [p for p in _PROBE_REGISTRY if p.filter_keys & filter_set]
    if not active:
        return {"job_id": job_id, "probe": True, "metrics": {}}

    PSTART = "MACLI_PROBE_START"
    PEND   = "MACLI_PROBE_END"

    script_parts = ["#!/bin/bash"]
    for p in active:
        script_parts.append(f'echo "{PSTART}:{p.key}"')
        script_parts.append(p.shell)
        script_parts.append(f'echo "{PEND}:{p.key}"')
    script = "\n".join(script_parts)

    dprint(f"[dim]probe: 运行 {[p.key for p in active]} (单次连接, 后端={backend})[/dim]")

    def _attempt():
        if backend == "ssh":
            output, _ = _exec_script_ssh_capture(
                ssh_entries or [], script, task=task_name, timeout=timeout,
                identityfile=identityfile, ssh_opts=ssh_opts,
            )
        else:
            output, _ = _exec_script(sess, job_id, task_name, script, timeout=timeout)
        _raw_debug(f"probe raw output:\n{output}")
        found_any = any(f"{PSTART}:{p.key}" in output for p in active)
        m: dict = {}
        for p in active:
            section = ""
            s_mark = f"{PSTART}:{p.key}"
            e_mark = f"{PEND}:{p.key}"
            if s_mark in output:
                after = output.split(s_mark, 1)[1]
                section = after.split(e_mark, 1)[0] if e_mark in after else after
            kv = _probe_kv(section)
            dprint(f"[dim]probe [{p.key}] kv={kv}[/dim]")
            m.update(p.parse_fn(kv))
        return m, found_any

    t0 = time.monotonic()
    metrics, found = _attempt()
    if not found:
        dprint("[dim]probe: 未收到输出，重试中 (1/2)...[/dim]")
        metrics, found = _attempt()
    if not found:
        dprint("[dim]probe: 未收到输出，重试中 (2/2)...[/dim]")
        metrics, _ = _attempt()
    elapsed = time.monotonic() - t0

    return {"job_id": job_id, "probe": True, "probe_backend": backend,
            "probe_elapsed_s": round(elapsed, 2), "metrics": metrics}


def _usage_check_exec_access(api: "API", job_id: str, preferred_task: str = None) -> str:
    """检查 CloudShell 权限并返回 task_name，失败则 exit。"""
    status = api.get_exec_status(job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业 CloudShell 未就绪，无法使用 --probe[/red]")
            sys.exit(1)
    tasks = api.get_job_tasks(job_id)
    return _pick_log_task(tasks, preferred=preferred_task)


def cmd_usage(args):
    sess = _sess_or_exit()
    api  = API(sess)

    use_probe     = getattr(args, "probe", False)
    probe_backend = get_exec_backend() if use_probe else "cloudshell"
    filter_set    = _parse_metrics_filter(getattr(args, "metrics", None) or [])

    if args.job_id:
        if use_probe:
            if probe_backend == "ssh":
                port_cache = PortCache().load()
                try:
                    job_detail = api.get_job(args.job_id)
                    if not job_detail: sys.exit(1)
                    phase = job_detail.get("status", {}).get("phase", "")
                    probe_ssh_entries = resolve_ssh(api, args.job_id, phase, port_cache,
                                                    detail_hint=job_detail)
                    port_cache.save()
                except SessionExpiredError:
                    cprint("[yellow]WARN: session 已失效，使用缓存 SSH 端口进行探测[/yellow]")
                    probe_ssh_entries = port_cache.get(args.job_id)
                if not probe_ssh_entries:
                    cprint("[red]该作业暂无 SSH 信息，无法使用 SSH 后端 probe[/red]"); sys.exit(1)
                preferred = getattr(args, "task", None)
                task_name = preferred or probe_ssh_entries[0]["task"]
            else:
                probe_ssh_entries = None
                task_name = _usage_check_exec_access(api, args.job_id,
                                                     preferred_task=getattr(args, "task", None))
            result = _run_probes(sess, args.job_id, task_name, filter_set,
                                 timeout=getattr(args, "timeout", 60),
                                 backend=probe_backend, ssh_entries=probe_ssh_entries)
        else:
            result = _fetch_usage_result(api, args.job_id, args.minutes, args.step)
        if getattr(args, "json", False):
            _json_out(result)
            return
        console.print(Panel(
            _usage_panel_text(result, filter_set=filter_set),
            title=f"作业监控  {args.job_id}",
            border_style="cyan",
        ))
        if use_probe:
            pb = result.get("probe_backend", "cloudshell")
            el = result.get("probe_elapsed_s")
            el_str = f"  耗时 {el}s" if el is not None else ""
            cprint(f"[dim][probe] 实时单点采样  后端={pb}{el_str}[/dim]")
        else:
            cprint(f"[dim]时间范围: 最近 {args.minutes} 分钟，step={args.step}s[/dim]")
        return

    port_cache  = PortCache().load()
    concurrency = getattr(args, "concurrency", 8)
    # degraded[0] = True when session is expired; set on first SessionExpiredError
    degraded = [False]

    try:
        jobs = api.list_jobs(limit=args.limit).get("items", [])
        running_jobs = [j for j in jobs if j.get("status", {}).get("phase") == "Running"]
        port_cache.evict_non_running({j.get("metadata", {}).get("id", "")
                                       for j in running_jobs
                                       if j.get("metadata", {}).get("id")})
    except SessionExpiredError:
        if use_probe and probe_backend == "ssh":
            cprint("[yellow]WARN: session 已失效，使用缓存 SSH 端口进行探测[/yellow]")
            degraded[0] = True
            running_jobs = [
                {"metadata": {"id": jid, "name": jid[:8]}, "status": {"phase": "Running"}}
                for jid in port_cache._data
            ]
        else:
            raise

    def _fetch_one(job):
        meta        = job.get("metadata", {})
        st          = job.get("status",   {})
        job_id      = meta.get("id", "")
        name        = meta.get("name", "")
        create_time = meta.get("create_time")          # ms timestamp
        duration_ms = st.get("duration")               # ms
        try:
            if use_probe:
                if probe_backend == "ssh":
                    if not degraded[0]:
                        try:
                            job_detail = api.get_job(job_id)
                            if job_detail:
                                phase_p = job_detail.get("status", {}).get("phase", "")
                                probe_ssh_entries = resolve_ssh(api, job_id, phase_p,
                                                                port_cache, detail_hint=job_detail)
                            else:
                                probe_ssh_entries = []
                        except SessionExpiredError:
                            degraded[0] = True
                            probe_ssh_entries = port_cache.get(job_id) or []
                    else:
                        probe_ssh_entries = port_cache.get(job_id) or []
                    preferred = getattr(args, "task", None)
                    task_name = preferred or (probe_ssh_entries[0]["task"] if probe_ssh_entries else "worker-0")
                else:
                    probe_ssh_entries = None
                    task_name = _usage_check_exec_access(api, job_id,
                                                         preferred_task=getattr(args, "task", None))
                u = _run_probes(sess, job_id, task_name, filter_set,
                                timeout=getattr(args, "timeout", 60),
                                backend=probe_backend, ssh_entries=probe_ssh_entries)
            else:
                u = _fetch_usage_result(api, job_id, args.minutes, args.step)
        except Exception as e:
            dprint(f"[red]{job_id} 采集失败: {e}[/red]")
            u = {"metrics": {}}

        # SSH 端口：从共享 PortCache 读取（probe SSH 模式下 resolve_ssh 已更新缓存）
        port_entries = port_cache.get(job_id) or []
        ssh_port = ssh_ports_summary(port_entries)

        return {
            "job_id":       job_id,
            "name":         name,
            "ssh_port":     ssh_port,
            "create_time":  int(create_time) if create_time is not None else 0,
            "duration_ms":  int(duration_ms) if duration_ms is not None else 0,
            "collected_at": datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S"),
            "cpu":          u["metrics"].get("cpu_util",              {}).get("latest"),
            "mem":          u["metrics"].get("memory_used_megabytes", {}).get("latest"),
            "gpu":          u["metrics"].get("gpu_util",              {}).get("latest"),
            "gpu_mem":      u["metrics"].get("gpu_mem_used_megabytes",{}).get("latest"),
            "gpu_devices":  u["metrics"].get("gpu_devices", []),
        }

    import concurrent.futures
    rows_map = {}
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
        transient=True,
    ) as progress:
        ptask = progress.add_task(
            f"采集中（并发={concurrency}）...", total=len(running_jobs)
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            fut_to_idx = {pool.submit(_fetch_one, job): i
                          for i, job in enumerate(running_jobs)}
            for fut in concurrent.futures.as_completed(fut_to_idx):
                idx = fut_to_idx[fut]
                rows_map[idx] = fut.result()
                progress.advance(ptask)

    port_cache.save()
    rows = sorted(
        (rows_map[i] for i in range(len(running_jobs))),
        key=lambda r: r["create_time"], reverse=True,
    )

    # 为 JSON/显示补充格式化字段
    for r in rows:
        r["create_time_str"] = ts_to_str(r["create_time"]) if r["create_time"] else "--"
        r["duration_str"]    = ms_to_hms(r["duration_ms"])  if r["duration_ms"]  else "--"

    if getattr(args, "json", False):
        _json_out({
            "minutes": args.minutes,
            "step": args.step,
            "jobs": rows,
        })
        return

    def _gpu_color(util, vram_used_mb, vram_total_mb):
        """Green = idle (util==0 AND vram<3%); Red = heavy; Yellow = in-use."""
        vram_pct = vram_used_mb / vram_total_mb if vram_total_mb else 0
        if util == 0 and vram_pct < 0.03:
            return "green"
        if util >= 0.8 or vram_pct >= 0.8:
            return "red"
        return "yellow"

    def _fmt_gpu_cell(r):
        devs = r.get("gpu_devices", [])
        if devs:
            parts = []
            for d in devs:
                color = _gpu_color(d.get("util", 0) or 0,
                                   d.get("vram_used_mb", 0) or 0,
                                   d.get("vram_total_mb", 1) or 1)
                parts.append(f"[{color}]GPU{d['index']} {(d.get('util') or 0)*100:.0f}%[/{color}]")
            return "\n".join(parts)
        return _fmt_usage_value("gpu_util", r["gpu"])

    def _fmt_vram_cell(r):
        devs = r.get("gpu_devices", [])
        if devs:
            parts = []
            for d in devs:
                color = _gpu_color(d.get("util", 0) or 0,
                                   d.get("vram_used_mb", 0) or 0,
                                   d.get("vram_total_mb", 1) or 1)
                parts.append(
                    f"[{color}]GPU{d['index']} "
                    f"{d.get('vram_used_mb') or 0:.0f}/{d.get('vram_total_mb') or 0:.0f}MB"
                    f"[/{color}]"
                )
            return "\n".join(parts)
        return _fmt_usage_value("gpu_mem_used_megabytes", r["gpu_mem"])

    # 多作业表格：按 filter_set 决定显示哪些列
    col_defs = [
        ("cpu",  "CPU",  lambda r: _fmt_usage_value("cpu_util", r["cpu"])),
        ("mem",  "内存", lambda r: _fmt_usage_value("memory_used_megabytes", r["mem"])),
        ("gpu",  "GPU",  _fmt_gpu_cell),
        ("vram", "显存", _fmt_vram_cell),
    ]
    active_cols = [(label, fmt) for key, label, fmt in col_defs if key in filter_set]

    t = Table(title="Running 作业最近 usage", header_style="bold cyan", show_lines=True)
    t.add_column("名称", style="green")
    t.add_column("JOB_ID", style="dim")
    t.add_column("SSH端口", style="cyan", no_wrap=True)
    t.add_column("创建时间", style="dim", no_wrap=True)
    t.add_column("运行时长", style="dim", no_wrap=True)
    for label, _ in active_cols:
        t.add_column(label)
    t.add_column("采集时间", style="dim", no_wrap=True)
    for row in rows:
        t.add_row(
            row["name"], row["job_id"], row.get("ssh_port", "—"),
            row.get("create_time_str", "--"), row.get("duration_str", "--"),
            *[fmt(row) for _, fmt in active_cols],
            row.get("collected_at", ""),
        )
    console.print(t)
    if use_probe:
        cprint("[dim]仅显示 Running 作业最近 usage；[probe] 实时单点采样[/dim]")
    else:
        cprint(f"[dim]仅显示 Running 作业最近 usage；时间范围: 最近 {args.minutes} 分钟，step={args.step}s[/dim]")



def _ws_recv_exact(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("socket closed")
        buf += chunk
    return buf


def _ws_read_frame(sock):
    b1, b2 = _ws_recv_exact(sock, 2)
    opcode = b1 & 0x0F
    masked = (b2 >> 7) & 1
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", _ws_recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _ws_recv_exact(sock, 8))[0]
    mask = b""
    if masked:
        mask = _ws_recv_exact(sock, 4)
    payload = _ws_recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def _ws_send_frame(sock, payload: bytes, opcode: int = 2):
    fin_opcode = 0x80 | (opcode & 0x0F)
    mask_bit = 0x80
    n = len(payload)
    header = bytearray([fin_opcode])
    if n < 126:
        header.append(mask_bit | n)
    elif n < (1 << 16):
        header.append(mask_bit | 126)
        header.extend(struct.pack("!H", n))
    else:
        header.append(mask_bit | 127)
        header.extend(struct.pack("!Q", n))
    mask_key = os.urandom(4)
    header.extend(mask_key)
    masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    sock.sendall(bytes(header) + masked)


def _open_exec_ws(sess: ConsoleSession, job_id: str, task_name: str, command: str = "/bin/bash"):
    host = "console.huaweicloud.com"
    path = (
        f"/modelarts/rest/v2/{sess.project_id}/training-jobs/{job_id}/exec"
        f"?task_id={urllib.parse.quote(task_name)}&command={urllib.parse.quote(command)}"
    )
    proto = (
        f"origin|https%3A%2F%2Fconsole.huaweicloud.com, "
        f"cftk|{sess.cftk or ''}, "
        f"agencyid|{sess.agency_id or ''}, "
        f"projectname|{sess.region or ''}, "
        f"region|{sess.region or ''}"
    )
    key = base64.b64encode(os.urandom(16)).decode()
    cookie = "; ".join(f"{c.name}={c.value}" for c in sess.http.cookies)
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Protocol: {proto}\r\n"
        f"Origin: https://console.huaweicloud.com\r\n"
        f"User-Agent: Mozilla/5.0\r\n"
        f"Cookie: {cookie}\r\n"
        f"\r\n"
    )
    ctx = ssl.create_default_context()
    raw_sock = socket.create_connection((host, 443), timeout=10)
    sock = ctx.wrap_socket(raw_sock, server_hostname=host)
    sock.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(4096)
    header = resp.split(b"\r\n\r\n", 1)[0].decode("utf-8", errors="replace")
    if "101 Switching Protocols" not in header:
        sock.close()
        raise RuntimeError(f"CloudShell websocket 握手失败: {header[:300]}")
    sock.settimeout(2)
    return sock


def _send_resize(sock, cols: int, rows: int):
    msg = json.dumps({"Width": cols, "Height": rows}).encode()
    _ws_send_frame(sock, b"\x04" + msg, opcode=2)


def cmd_shell(args):
    sess = _sess_or_exit()
    api  = API(sess)

    status = api.get_exec_status(args.job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业当前不允许打开 CloudShell[/red]")
            sys.exit(1)

    tasks = api.get_job_tasks(args.job_id)
    task_name = _pick_log_task(tasks, preferred=args.task, interactive=True)

    dprint("[cyan]正在连接 CloudShell...[/cyan]")
    sock = _open_exec_ws(sess, args.job_id, task_name, command="/bin/bash")
    dprint("[green]✓ 已连接[/green] [dim](退出热键: Ctrl-])[/dim]")
    dprint(f"[dim][shell] heartbeat interval = {max(0.5, float(args.heartbeat))}s[/dim]")

    stop = {"value": False}
    old_tty = termios.tcgetattr(sys.stdin.fileno())

    def reader():
        try:
            while not stop["value"]:
                try:
                    opcode, payload = _ws_read_frame(sock)
                except TimeoutError:
                    continue
                except socket.timeout:
                    continue
                if opcode == 8:
                    _raw_debug("websocket close frame received")
                    break
                if opcode in (1, 2) and payload:
                    # cloudShell 下行目前已确认主要是 0x01 + 终端字节流
                    if opcode == 2 and payload[:1] == b"\x01":
                        _status_debug(f"recv frame: opcode={opcode} ch=01 len={len(payload)-1}")
                        payload = payload[1:]
                    else:
                        _status_debug(f"recv frame: opcode={opcode} raw-len={len(payload)} head={payload[:8].hex()}")
                    if payload:
                        os.write(sys.stdout.fileno(), payload)
                        sys.stdout.flush()
        except Exception as e:
            _raw_debug(f"reader error: {type(e).__name__}: {e}")
        stop["value"] = True

    _heart_toggle = {"on": True}

    def _blink_heart():
        sym = "♥" if _heart_toggle["on"] else "♡"
        _heart_toggle["on"] = not _heart_toggle["on"]
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 80
        # Save cursor → jump to top-right → print heart → restore cursor
        indicator = f"\033[s\033[1;{cols}H\033[31m{sym}\033[m\033[u"
        os.write(sys.stderr.fileno(), indicator.encode())

    def heartbeat_sender():
        try:
            interval = max(0.5, float(args.heartbeat))
            while not stop["value"]:
                time.sleep(interval)
                if stop["value"]:
                    break
                try:
                    _ws_send_frame(sock, b"\x00", opcode=2)
                    if _VERBOSE:
                        _blink_heart()
                except Exception as e:
                    _raw_debug(f"heartbeat failed: {type(e).__name__}: {e}")
                    stop["value"] = True
                    break
        except Exception as e:
            _raw_debug(f"heartbeat thread error: {type(e).__name__}: {e}")

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    hb = threading.Thread(target=heartbeat_sender, daemon=True)
    hb.start()

    old_sigwinch = signal.getsignal(signal.SIGWINCH)

    def _on_resize(signum, frame):
        try:
            sz = os.get_terminal_size()
            _send_resize(sock, sz.columns, sz.lines)
        except Exception:
            pass

    try:
        tty.setraw(sys.stdin.fileno())
        signal.signal(signal.SIGWINCH, _on_resize)
        try:
            sz = os.get_terminal_size()
            _send_resize(sock, sz.columns, sz.lines)
            _raw_debug(f"initial resize sent: {sz.columns}x{sz.lines}")
        except Exception as e:
            _raw_debug(f"resize send failed: {type(e).__name__}: {e}")
        # 打开 stdin 通道；不主动补回车，避免重复打印 prompt
        try:
            _ws_send_frame(sock, b"\x00", opcode=2)
            _raw_debug("init stdin frame sent")
        except Exception as e:
            _raw_debug(f"init send failed: {type(e).__name__}: {e}")
        while not stop["value"]:
            r, _, _ = select.select([sys.stdin.fileno()], [], [], 0.1)
            if not r:
                continue
            data = os.read(sys.stdin.fileno(), 1)
            if not data:
                break
            # cloudShell 上行消息格式：0x00 + stdin字节
            _ws_send_frame(sock, b"\x00" + data, opcode=2)
    finally:
        signal.signal(signal.SIGWINCH, old_sigwinch)
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_tty)
        try:
            _ws_send_frame(sock, b"\x00exit\r", opcode=2)
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass
        dprint("\n[dim]CloudShell 已退出[/dim]")


def _exec_script(
    sess: "ConsoleSession",
    job_id: str,
    task_name: str,
    script: str,
    timeout: int = 120,
    cwd: str = None,
) -> "tuple[str, int]":
    """
    底层传输：通过 CloudShell WebSocket 执行 script，返回 (stdout文本, exit_code)。
    脚本经 base64 编码传输，支持多行/特殊字符/heredoc 等任意内容。
    """
    import base64 as _b64
    script_b64 = _b64.b64encode(script.encode()).decode()

    START_MARKER = "MACLI_EXEC_START_7f3a9"
    EXIT_MARKER  = "MACLI_EXEC_EXIT_7f3a9"
    TMP_B64      = "/tmp/.macli_exec_b64_$$"

    CHUNK = 512
    chunks = [script_b64[i:i+CHUNK] for i in range(0, len(script_b64), CHUNK)]

    setup_lines = [
        "stty -echo; PS1=''; PS2=''\r",
        f"TMP={TMP_B64}; rm -f \"$TMP\"\r",
    ]
    for ch in chunks:
        setup_lines.append(f"printf '%s' '{ch}' >> \"$TMP\"\r")

    run_parts = ["base64 -d \"$TMP\" | bash"]
    if cwd:
        cwd_esc = cwd.replace("'", "'\\''")
        run_parts = [f"cd '{cwd_esc}' &&"] + run_parts

    setup_lines.append(
        f"echo {START_MARKER}; "
        + " ".join(run_parts)
        + f"; echo {EXIT_MARKER}:$?; rm -f \"$TMP\"; exit\r"
    )

    sock = _open_exec_ws(sess, job_id, task_name, command="/bin/bash")

    buf       = bytearray()
    exit_code = [None]
    done      = threading.Event()

    def _reader():
        try:
            while not done.is_set():
                try:
                    opcode, payload = _ws_read_frame(sock)
                except (TimeoutError, socket.timeout):
                    continue
                if opcode == 8:
                    break
                if opcode in (1, 2) and payload:
                    if opcode == 2 and payload[:1] == b"\x01":
                        payload = payload[1:]
                    if payload:
                        buf.extend(payload)
                        if EXIT_MARKER.encode() in buf:
                            m = re.search(rf"{EXIT_MARKER}:(\d+)",
                                          buf.decode("utf-8", errors="replace"))
                            if m:
                                exit_code[0] = int(m.group(1))
                            done.set()
        except Exception as e:
            _raw_debug(f"_exec_script reader: {type(e).__name__}: {e}")
        done.set()

    def _heartbeat():
        while not done.is_set():
            done.wait(timeout=5)
            if done.is_set():
                break
            try:
                _ws_send_frame(sock, b"\x00", opcode=2)
            except Exception:
                break

    threading.Thread(target=_reader,    daemon=True).start()
    threading.Thread(target=_heartbeat, daemon=True).start()

    time.sleep(0.8)
    for line in setup_lines:
        _ws_send_frame(sock, b"\x00" + line.encode(), opcode=2)
        time.sleep(0.05)

    done.wait(timeout=timeout)
    if not done.is_set():
        _raw_debug(f"_exec_script timeout after {timeout}s")

    try:
        sock.close()
    except Exception:
        pass

    raw = buf.decode("utf-8", errors="replace")
    if START_MARKER in raw:
        raw = raw.split(START_MARKER, 1)[1].lstrip("\r\n")
    if EXIT_MARKER in raw:
        raw = raw[:raw.index(EXIT_MARKER)]
    clean = re.sub(r"\x1b\[[0-9;]*[A-Za-z]|\r", "", raw)

    return clean, exit_code[0] if exit_code[0] is not None else -1


def _build_ssh_cmd(ssh_entries: list, task: str = None,
                   identityfile: str = None, ssh_opts: list = None) -> "tuple[list, str, str, int]":
    """构造 SSH 命令基础参数，返回 (ssh_base_cmd, user, host, port)。
    ssh_entries 为已 enrich 的列表（来自 resolve_ssh）。
    """
    if not ssh_entries:
        cprint("[red]该作业暂无 SSH 信息，无法使用 SSH 后端[/red]")
        sys.exit(1)
    if task:
        entry = next((e for e in ssh_entries if e.get("task") == task), None)
        if entry is None:
            cprint(f"[red]未找到任务：{task}，可用：{[e['task'] for e in ssh_entries]}[/red]")
            sys.exit(1)
    else:
        entry = ssh_entries[0]
        if len(ssh_entries) > 1:
            dprint(f"[dim]自动选择 {entry['task']}（共 {len(ssh_entries)} 个节点，可用 --task 指定）[/dim]")
    user, host, port = _parse_ssh_url(entry["url"])
    if not host:
        cprint(f"[red]无法解析 SSH URL：{entry['url']}[/red]")
        sys.exit(1)
    if not identityfile:
        _, default = load_identityfiles()
        identityfile = default
    if not identityfile:
        cprint("[red]未指定 SSH 密钥，请用 --identityfile 或 macli identityfile default --set <PATH>[/red]")
        sys.exit(1)
    identity_path = resolve_identityfile(identityfile)
    cmd = ["ssh", "-p", str(port), "-i", identity_path,
           "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
           "-o", "LogLevel=ERROR", "-o", "BatchMode=yes"]
    if ssh_opts:
        cmd += ssh_opts
    return cmd, user, host, port


def _exec_script_ssh_capture(
    ssh_entries: list,
    script: str,
    task: str = None,
    timeout: int = 300,
    cwd: str = None,
    identityfile: str = None,
    ssh_opts: list = None,
) -> "tuple[str, int]":
    """通过 SSH 执行脚本并捕获输出，返回 (stdout文本, exit_code)。用于 probe 等需要解析输出的场景。"""
    ssh_base, user, host, _ = _build_ssh_cmd(ssh_entries, task=task,
                                              identityfile=identityfile, ssh_opts=ssh_opts)
    if cwd:
        cwd_esc = cwd.replace("'", "'\\''")
        remote_cmd = f"cd '{cwd_esc}' && bash -s"
    else:
        remote_cmd = "bash -s"
    cmd = ssh_base + [f"{user}@{host}", remote_cmd]
    dprint(f"[dim]{' '.join(cmd)}[/dim]")
    try:
        result = _subprocess.run(cmd, input=script.encode(),
                                 stdout=_subprocess.PIPE, stderr=_subprocess.PIPE,
                                 timeout=timeout)
        return result.stdout.decode("utf-8", errors="replace"), result.returncode
    except _subprocess.TimeoutExpired:
        return "", -1
    except Exception as e:
        dprint(f"[red]SSH capture 失败: {e}[/red]")
        return "", -1


def _exec_script_ssh(
    ssh_entries: list,
    script: str,
    task: str = None,
    timeout: int = 300,
    cwd: str = None,
    identityfile: str = None,
    ssh_opts: list = None,
) -> int:
    """通过原生 SSH 执行脚本，stdout/stderr 直接流向终端，返回 exit_code。"""
    ssh_base, user, host, _ = _build_ssh_cmd(ssh_entries, task=task,
                                              identityfile=identityfile, ssh_opts=ssh_opts)
    if cwd:
        cwd_esc = cwd.replace("'", "'\\''")
        remote_cmd = f"cd '{cwd_esc}' && bash -s"
    else:
        remote_cmd = "bash -s"
    ssh_cmd = ssh_base + [f"{user}@{host}", remote_cmd]
    dprint(f"[dim]{' '.join(ssh_cmd)}[/dim]")
    try:
        result = _subprocess.run(ssh_cmd, input=script.encode(), timeout=timeout)
        return result.returncode
    except _subprocess.TimeoutExpired:
        cprint(f"[red]SSH 执行超时（{timeout}s）[/red]")
        return -1
    except Exception as e:
        cprint(f"[red]SSH 执行失败: {e}[/red]")
        return -1


def _exec_one(args, sess, api, job_id: str, backend: str, script: str) -> int:
    """对单个 job_id 执行脚本，返回退出码。"""
    timeout = getattr(args, "timeout", 300)
    cwd     = getattr(args, "cwd", None)
    task    = getattr(args, "task", None)

    dprint(f"[dim]_exec_one: job={job_id[:8]}… backend={backend} cwd={cwd} timeout={timeout}[/dim]")

    if backend == "ssh":
        job = api.get_job(job_id)
        if not job:
            return 1
        phase = job.get("status", {}).get("phase", "")
        port_cache = PortCache().load()
        ssh_entries = resolve_ssh(api, job_id, phase, port_cache, detail_hint=job)
        port_cache.save()
        if not ssh_entries:
            cprint("[red]该作业暂无 SSH 信息，无法使用 SSH 后端[/red]")
            return 1
        return _exec_script_ssh(
            ssh_entries, script,
            task=task, timeout=timeout, cwd=cwd,
            identityfile=getattr(args, "identityfile", None),
            ssh_opts=getattr(args, "ssh_opts", None),
        )

    status = api.get_exec_status(job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业当前不允许执行命令（CloudShell 未就绪）[/red]")
            return 1

    tasks     = api.get_job_tasks(job_id)
    task_name = _pick_log_task(tasks, preferred=task)
    dprint(f"[cyan]正在连接（task={task_name}）...[/cyan]")
    output, code = _exec_script(sess, job_id, task_name, script, timeout=timeout, cwd=cwd)
    dprint("[green]✓ 完成[/green]")
    sys.stdout.write(output)
    if output and not output.endswith("\n"):
        sys.stdout.write("\n")
    return code


def _exec_batch(args, backend: str, job_ids: list):
    """对多个作业顺序执行同一脚本（由管道传入 ID）。"""
    sess = _sess_or_exit()
    api  = API(sess)

    # 读取脚本内容（脚本来源不含 stdin，已由调用方保证）
    if getattr(args, "script_file", None):
        try:
            with open(args.script_file, "r") as f:
                script = f.read()
        except OSError as e:
            cprint(f"[red]读取脚本文件失败: {e}[/red]"); sys.exit(1)
    elif getattr(args, "inline_cmd", None):
        parts = args.inline_cmd
        if parts and parts[0] == "--":
            parts = parts[1:]
        script = " ".join(parts)
    else:
        cprint("[red]批量模式下请使用 -- <cmd> 或 --script 指定命令[/red]"); sys.exit(1)

    dprint(f"[dim]_exec_batch: {len(job_ids)} 个作业，backend={backend}[/dim]")
    exit_codes = []
    for job_id in job_ids:
        job = api.get_job(job_id)
        label = job.get("metadata", {}).get("name", job_id) if job else job_id
        cprint(f"\n[bold cyan]══ {label} ({job_id[:8]}…) ══[/bold cyan]")
        code = _exec_one(args, sess, api, job_id, backend, script)
        dprint(f"[dim]  {label}: exit {code}[/dim]")
        exit_codes.append(code)

    failed = sum(1 for c in exit_codes if c != 0)
    if failed:
        cprint(f"\n[yellow]完成：{len(job_ids) - failed}/{len(job_ids)} 个作业成功[/yellow]")
        sys.exit(1)
    else:
        cprint(f"\n[green]完成：全部 {len(job_ids)} 个作业执行成功[/green]")


def cmd_exec(args):
    """在作业容器内执行命令，支持 cloudshell 和 ssh 两种后端。"""

    # ── 确定并记忆后端 ─────────────────────────────────────────
    backend_arg = getattr(args, "backend", None)
    if backend_arg:
        set_exec_backend(backend_arg)
        backend = backend_arg
    else:
        backend = get_exec_backend()

    # 无 JOB_ID：检查是否有管道 ID（且有内联命令），否则仅保存后端设置
    if not getattr(args, "job_id", None):
        # --stdin 读脚本与管道读 ID 互斥：有 --stdin 时 stdin 留给脚本
        piped_ids = [] if getattr(args, "use_stdin", False) else _read_piped_ids()
        has_cmd   = (getattr(args, "script_file", None)
                     or getattr(args, "use_stdin", False)
                     or getattr(args, "inline_cmd", None))
        if piped_ids and has_cmd:
            # 批量模式：对每个 JOB_ID 顺序执行同一条命令
            args.job_id = None          # 占位，下面按 job_id 循环
            _exec_batch(args, backend, piped_ids)
            return
        if backend_arg:
            cprint(f"[green]✓ 默认 exec 后端已设为：{backend}[/green]")
        else:
            cprint(f"当前 exec 后端：[cyan]{backend}[/cyan]")
        return

    sess = _sess_or_exit()
    api  = API(sess)

    # ── 确定脚本内容 ───────────────────────────────────────────
    if getattr(args, "script_file", None):
        try:
            with open(args.script_file, "r") as f:
                script = f.read()
        except OSError as e:
            cprint(f"[red]读取脚本文件失败: {e}[/red]")
            sys.exit(1)
    elif getattr(args, "use_stdin", False):
        script = sys.stdin.read()
    elif getattr(args, "inline_cmd", None):
        parts = args.inline_cmd
        if parts and parts[0] == "--":
            parts = parts[1:]
        script = " ".join(parts)
    else:
        cprint("[red]请指定命令：使用 -- <cmd>、--script <file> 或 --stdin[/red]")
        sys.exit(1)

    sys.exit(_exec_one(args, sess, api, args.job_id, backend, script))


def cmd_copy(args):
    sess = _sess_or_exit()
    api  = API(sess)

    # 支持按 JOB_ID 或 --src-name 指定源作业
    if getattr(args, "src_name", None):
        # 按名称查找：从作业列表里匹配
        data = api.list_jobs(limit=50)
        items = data.get("items", [])
        matched = [j for j in items
                   if j.get("metadata", {}).get("name", "") == args.src_name]
        if not matched:
            cprint(f"[red]未找到名称为 '{args.src_name}' 的作业[/red]")
            sys.exit(1)
        if len(matched) > 1:
            cprint(f"[yellow]找到 {len(matched)} 个同名作业，使用最新的一个[/yellow]")
        job = matched[0]
        args.job_id = job.get("metadata", {}).get("id", "")
    else:
        job = api.get_job(args.job_id)
        if not job: sys.exit(1)

    meta      = job.get("metadata", {})
    spec      = job.get("spec", {})
    algo      = job.get("algorithm", {})
    cur_flavor= spec.get("resource", {}).get("flavor_id", "")
    cur_cmd   = algo.get("command", "")
    cur_name  = meta.get("name", "")
    cur_desc  = meta.get("description", "")

    # 计算新名称（用于预览）
    if args.name:
        new_name = args.name
    else:
        clean    = re.sub(r'-copy-\d+$', '', cur_name)
        new_name = f"{clean}-copy-{int(time.time()) % 100000}"

    # --json 模式：跳过所有交互和表格，直接提交
    if not getattr(args, "json", False):
        full_cmd    = args.command or cur_cmd
        cmd_preview = full_cmd.replace("\n", " ; ")[:60]
        cmd_suffix  = "..." if len(full_cmd) > 60 else ""
        desc_show   = args.description if args.description is not None else (cur_desc or "(无)")
        gpu_show    = str(args.gpu_count) if args.gpu_count else "(不变)"
        console.print(Panel(
            f"[bold]原作业:[/bold]       {cur_name}\n"
            f"[bold]新名称:[/bold]       {new_name}\n"
            f"[bold]描述:[/bold]         {desc_show}\n"
            f"[bold]当前规格:[/bold]     {cur_flavor}\n"
            f"[bold]新GPU卡数:[/bold]    {gpu_show}\n"
            f"[bold]启动命令:[/bold]     {cmd_preview}{cmd_suffix}",
            title="复制训练作业", border_style="yellow"))

        if not args.yes:
            ok = Confirm.ask("确认创建？")
            if not ok: cprint("[yellow]已取消[/yellow]"); return

    # --command-file 优先于 --command
    if args.command_file:
        try:
            args.command = Path(args.command_file).read_text(encoding="utf-8").strip()
            if not getattr(args, "json", False):
                dprint(f"[green]✓ 从 {args.command_file} 读取启动命令[/green]")
        except Exception as e:
            cprint(f"[red]读取命令文件失败: {e}[/red]"); sys.exit(1)

    if not getattr(args, "json", False):
        dprint("[cyan]提交中...[/cyan]")
    result = api.copy_job(
        args.job_id,
        new_gpu_count = args.gpu_count,
        new_name      = args.name,
        description   = args.description,
        command       = args.command,
    )

    if result:
        if getattr(args, "json", False):
            _json_out(job_to_dict(result))
        else:
            nm = result.get("metadata", {})
            cprint(f"[green]✓ 创建成功！[/green]")
            cprint(f"  名称: {nm.get('name','')}")
            cprint(f"  ID:   {nm.get('id','')}")
            cprint(f"  状态: {result.get('status',{}).get('phase','等待中')}")
    else:
        cprint("[red]创建失败[/red]")
        sys.exit(1)


def cmd_stop(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job_ids = list(getattr(args, "job_ids", None) or []) or _read_piped_ids()
    if not job_ids:
        cprint("[red]请提供 JOB_ID 或通过管道传入（macli query ... | macli stop）[/red]")
        sys.exit(1)

    # 收集作业信息（去除不存在的）
    jobs_info = []
    for job_id in job_ids:
        job = api.get_job(job_id)
        if not job:
            cprint(f"[red]未找到作业 {job_id}，跳过[/red]")
            continue
        jobs_info.append((job_id,
                          job.get("metadata", {}).get("name", ""),
                          job.get("status", {}).get("phase", "")))

    if not jobs_info:
        cprint("[yellow]没有可终止的作业[/yellow]"); return

    # 预览
    t = Table(title=f"即将终止 {len(jobs_info)} 个作业", header_style="bold red")
    t.add_column("名称", style="green")
    t.add_column("ID",   style="dim")
    t.add_column("状态")
    for job_id, name, phase in jobs_info:
        color = STATUS_COLOR.get(phase, "white")
        t.add_row(name, job_id, f"[{color}]{phase}[/{color}]")
    console.print(t)

    if not args.yes:
        ok = Confirm.ask(f"[red]确认终止以上 {len(jobs_info)} 个作业？[/red]")
        if not ok: cprint("[yellow]已取消[/yellow]"); return

    ok_count = 0
    for job_id, name, _ in jobs_info:
        if api.stop_job(job_id):
            cprint(f"[green]✓ 已发送终止指令: {name}[/green]")
            ok_count += 1
        else:
            cprint(f"[red]✗ 终止失败: {name}[/red]")
    if len(jobs_info) > 1:
        cprint(f"[green]完成：{ok_count}/{len(jobs_info)} 个作业已终止[/green]")


def cmd_delete(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job_ids = list(getattr(args, "job_ids", None) or []) or _read_piped_ids()
    if not job_ids:
        cprint("[red]请提供 JOB_ID 或通过管道传入（macli query ... | macli delete）[/red]")
        sys.exit(1)

    # 收集所有作业信息
    jobs_info = []
    for job_id in job_ids:
        job = api.get_job(job_id)
        if not job:
            cprint(f"[red]未找到作业 {job_id}，跳过[/red]")
            continue
        name  = job.get("metadata", {}).get("name", "")
        phase = job.get("status", {}).get("phase", "")
        if phase == "Running" and not args.force:
            cprint(f"[red]{name} 正在运行中，跳过（使用 -f 强制删除）[/red]")
            continue
        jobs_info.append((job_id, name, phase))

    if not jobs_info:
        cprint("[yellow]没有可删除的作业[/yellow]"); return

    # 预览列表
    t = Table(title=f"即将删除 {len(jobs_info)} 个作业", header_style="bold red")
    t.add_column("名称", style="green")
    t.add_column("ID",   style="dim")
    t.add_column("状态")
    for job_id, name, phase in jobs_info:
        color = "red" if phase == "Running" else STATUS_COLOR.get(phase, "white")
        t.add_row(name, job_id, f"[{color}]{phase}[/{color}]")
    console.print(t)
    if any(ph == "Running" for _, _, ph in jobs_info):
        cprint("[bold red]⚠ 包含运行中的作业，将被强制删除！[/bold red]")
    cprint("[red]此操作无法恢复！[/red]")
    if not args.yes:
        prompt = f"确认删除以上 {len(jobs_info)} 个作业？"
        ok = Confirm.ask(f"[red]{prompt}[/red]")
        if not ok: cprint("[yellow]已取消[/yellow]"); return

    # 逐个删除
    ok_count = 0
    for job_id, name, phase in jobs_info:
        if api.delete_job(job_id):
            cprint(f"[green]✓ 已删除: {name}[/green]")
            ok_count += 1
        else:
            cprint(f"[red]✗ 删除失败: {name}[/red]")

    cprint(f"[green]完成：{ok_count}/{len(jobs_info)} 个作业已删除[/green]")


def cmd_identityfile(args):
    files, default = load_identityfiles()

    if args.if_cmd == "add":
        path = str(Path(args.path).expanduser())
        if not os.path.exists(path):
            cprint(f"[yellow]警告：文件不存在：{path}[/yellow]")
        name = args.name if args.name else Path(path).stem
        if name in files and files[name] != path:
            cprint(f"[yellow]名称 '{name}' 已存在（{files[name]}），将被覆盖[/yellow]")
        files[name] = path
        # 若尚无默认，自动设为第一个添加的
        if default is None:
            default = name
        save_identityfiles(files, default)
        cprint(f"[green]✓ 已添加：{name} → {path}[/green]")
        if default == name:
            cprint(f"[dim]（已设为默认密钥）[/dim]")

    elif args.if_cmd == "remove":
        name = args.name
        if name not in files:
            cprint(f"[red]未找到密钥：{name}[/red]")
            sys.exit(1)
        del files[name]
        if default == name:
            default = next(iter(files), None)
            if default:
                cprint(f"[yellow]默认密钥已更新为：{default}[/yellow]")
            else:
                cprint("[yellow]已无默认密钥[/yellow]")
        save_identityfiles(files, default)
        cprint(f"[green]✓ 已移除：{name}[/green]")

    elif args.if_cmd == "list":
        if not files:
            cprint("[dim]暂无已保存的 SSH 密钥[/dim]")
            return
        t = Table(header_style="bold cyan", show_lines=False)
        t.add_column("名称", style="cyan")
        t.add_column("路径")
        t.add_column("默认", width=4)
        for n, p in files.items():
            mark = "[green]✓[/green]" if n == default else ""
            exists_hint = "" if os.path.exists(Path(p).expanduser()) else " [red](文件不存在)[/red]"
            t.add_row(n, p + exists_hint, mark)
        console.print(t)

    elif args.if_cmd == "default":
        if args.set:
            name_or_path = args.set
            # 若是已知名称
            if name_or_path in files:
                default = name_or_path
            else:
                # 视为路径，自动以文件名为键添加（若未注册）
                path = str(Path(name_or_path).expanduser())
                stem = Path(name_or_path).stem
                if stem not in files:
                    files[stem] = path
                default = stem
            save_identityfiles(files, default)
            cprint(f"[green]✓ 默认密钥已设为：{default} ({files.get(default, name_or_path)})[/green]")
        else:
            if default and default in files:
                cprint(f"默认密钥：[cyan]{default}[/cyan] → {files[default]}")
            elif default:
                cprint(f"默认密钥名称：[cyan]{default}[/cyan]（未在列表中，将直接用作路径）")
            else:
                cprint("[dim]未设置默认密钥，可用 macli identityfile default --set <PATH/NAME>[/dim]")


def cmd_ssh(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job = api.get_job(args.job_id)
    if not job:
        sys.exit(1)
    phase = job.get("status", {}).get("phase", "")

    port_cache = PortCache().load()
    ssh_list = resolve_ssh(api, args.job_id, phase, port_cache, detail_hint=job)
    port_cache.save()

    if not ssh_list:
        cprint("[red]该作业暂无 SSH 信息（未运行或不是调试模式）[/red]")
        sys.exit(1)

    if args.task:
        entry = next((e for e in ssh_list if e.get("task") == args.task), None)
        if entry is None:
            cprint(f"[red]未找到任务：{args.task}，可用：{[e['task'] for e in ssh_list]}[/red]")
            sys.exit(1)
    elif len(ssh_list) == 1:
        entry = ssh_list[0]
    else:
        cprint("[bold]可用 SSH 节点：[/bold]")
        for i, e in enumerate(ssh_list, 1):
            cprint(f"  [cyan]{i}.[/cyan] {e['task']}  [dim]{e['url']}[/dim]")
        while True:
            choice = input(f"\n请选择节点 (1-{len(ssh_list)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(ssh_list):
                entry = ssh_list[int(choice) - 1]
                break
            cprint("[red]输入无效，请重试[/red]")

    ssh_base, user, host, port = _build_ssh_cmd(
        ssh_list, task=args.task,
        identityfile=getattr(args, "identityfile", None),
        ssh_opts=getattr(args, "ssh_opts", None),
    )
    ssh_cmd = ssh_base + [f"{user}@{host}"]
    # ssh mode: no remote command (interactive shell), drop BatchMode
    ssh_cmd = [a for a in ssh_cmd if a != "BatchMode=yes"]

    cprint(f"[dim]连接：{entry['task']}  {' '.join(ssh_cmd)}[/dim]")
    os.execvp("ssh", ssh_cmd)


def _autologin_print_cfg(cfg: dict):
    """打印自动登录配置摘要。"""
    webhook_url = cfg.get("webhook_url", "")
    ntfy_topic  = cfg.get("ntfy_topic", "")
    if webhook_url:
        cprint(f"  OTP 通道  : [cyan]webhook → {webhook_url}[/cyan]")
    elif ntfy_topic:
        cprint(f"  OTP 通道  : [cyan]ntfy → {ntfy_topic}[/cyan]")
    else:
        cprint(f"  OTP 通道  : [dim]未配置[/dim]")
    cprint(f"  最大重试次数: {cfg.get('max_retries', 3)}")
    cprint(f"  验证码等待 : {cfg.get('otp_wait_secs', 120)} 秒")
    cprint(f"  熔断阈值   : {cfg.get('circuit_breaker', 3)} 次连续失败")


def cmd_autologin(args):
    """查询/启用/停用会话过期时的自动重新登录（keyring 账号密码 + webhook/ntfy OTP 通道）。"""
    import getpass as _getpass
    import secrets as _secrets

    action = getattr(args, "action", None) or "status"

    # ── status (with optional inline config update) ───────────
    if action == "status":
        cfg = _load_auto_login_cfg()
        changed = False
        if getattr(args, "retries", None) is not None:
            cfg["max_retries"] = args.retries
            changed = True
        if getattr(args, "timeout", None) is not None:
            cfg["otp_wait_secs"] = args.timeout
            changed = True
        if getattr(args, "circuit_breaker", None) is not None:
            cfg["circuit_breaker"] = args.circuit_breaker
            changed = True
        if getattr(args, "reset_topic", False):
            cfg["ntfy_topic"] = "macli-" + _secrets.token_hex(8)
            changed = True
        if changed:
            _save_auto_login_cfg(cfg)
            cprint("[green]✓ 配置已更新[/green]")
        if not cfg.get("enabled"):
            if cfg.get("circuit_tripped"):
                n = cfg.get("consecutive_failures", 0)
                cprint(f"[red]自动登录：[bold]已禁用（熔断 — 连续失败 {n} 次）[/bold][/red]")
            else:
                cprint("[yellow]自动登录：[bold]未启用[/bold][/yellow]")
        else:
            cprint("[green]自动登录：[bold]已启用[/bold][/green]")
            _autologin_print_cfg(cfg)
            failures = int(cfg.get("consecutive_failures", 0))
            if failures > 0:
                cprint(f"  [yellow]当前连续失败: {failures} 次[/yellow]")
        creds = _load_saved_creds()
        if creds.get("username"):
            cprint(f"  keyring 账号: [dim]{creds['username']} @ {creds['domain']}[/dim]")
        else:
            cprint("  keyring 账号: [dim]未保存[/dim]")
        return

    # ── disable ───────────────────────────────────────────────
    if action == "disable":
        cfg = _load_auto_login_cfg()
        cfg["enabled"] = False
        _save_auto_login_cfg(cfg)
        cprint("[green]✓ 自动登录已停用[/green]")
        return

    # ── enable ────────────────────────────────────────────────
    cfg = _load_auto_login_cfg()

    # Already enabled (or re-enabling after circuit trip): apply params, reset circuit state
    if cfg.get("enabled") or cfg.get("circuit_tripped"):
        changed = False
        if getattr(args, "retries", None) is not None:
            cfg["max_retries"] = args.retries
            changed = True
        if getattr(args, "timeout", None) is not None:
            cfg["otp_wait_secs"] = args.timeout
            changed = True
        if getattr(args, "circuit_breaker", None) is not None:
            cfg["circuit_breaker"] = args.circuit_breaker
            changed = True
        if getattr(args, "reset_topic", False):
            cfg["ntfy_topic"] = "macli-" + _secrets.token_hex(8)
            changed = True
        # Always clear circuit state when enable is explicitly called
        if cfg.get("circuit_tripped") or cfg.get("consecutive_failures", 0):
            cfg["circuit_tripped"]      = False
            cfg["consecutive_failures"] = 0
            cfg["enabled"]              = True
            changed = True
        if changed:
            _save_auto_login_cfg(cfg)
            cprint("[green]✓ 配置已更新[/green]")
        else:
            cprint("[dim]自动登录已启用，无参数变更[/dim]")
        _autologin_print_cfg(cfg)
        creds = _load_saved_creds()
        if creds.get("username"):
            cprint(f"  keyring 账号: [dim]{creds['username']} @ {creds['domain']}[/dim]")
        return

    # Not yet enabled: full setup flow
    creds = _load_saved_creds()
    if not (creds.get("domain") and creds.get("username") and creds.get("password")):
        cprint("[yellow]keyring 中无账号密码，请输入：[/yellow]")
        _domain   = input("租户名/原华为云账号: ").strip()
        _username = input("IAM 用户名/邮件地址: ").strip()
        _password = _getpass.getpass("IAM 用户密码: ")
        if not all([_domain, _username, _password]):
            cprint("[red]账号信息不完整，取消[/red]")
            return
        if _save_creds(_domain, _username, _password):
            cprint("[green]✓ 账号密码已保存至 keyring[/green]")
        else:
            cprint("[red]keyring 保存失败，无法启用自动登录[/red]")
            return
    else:
        cprint(f"[green]✓ 使用 keyring 账号：{creds['username']} @ {creds['domain']}[/green]")

    # OTP 通道选择：webhook（推荐）或 ntfy（降级）
    cprint("\n[bold]选择 OTP 通道：[/bold]")
    cprint("  [cyan]1.[/cyan] webhook（推荐 — 手机直接 POST 到 macli server）")
    cprint("  [cyan]2.[/cyan] ntfy.sh（公网中转）")
    otp_choice = input("\n请选择 (1/2) [1]: ").strip() or "1"

    if otp_choice == "1":
        # webhook 模式
        existing_url = cfg.get("webhook_url", "")
        default_url = existing_url or "http://localhost:8086"
        cprint(f"\n[dim]填写手机可访问的 macli server 地址（SSH 隧道/内网穿透/Tailscale 均可）[/dim]")
        webhook_url = input(f"webhook URL [{default_url}]: ").strip() or default_url
        # 清理末尾斜杠
        webhook_url = webhook_url.rstrip("/")
        ntfy_topic = cfg.get("ntfy_topic", "")  # 保留旧的，不再生成新的
    else:
        # ntfy 模式
        webhook_url = ""
        existing_topic = cfg.get("ntfy_topic", "")
        if getattr(args, "reset_topic", False) or not existing_topic:
            ntfy_topic = "macli-" + _secrets.token_hex(8)
            dprint(f"[dim]生成新 ntfy topic: {ntfy_topic}[/dim]")
        else:
            ntfy_topic = existing_topic
            dprint(f"[dim]复用已有 ntfy topic: {ntfy_topic}[/dim]")

    max_retries     = getattr(args, "retries",          None) or int(cfg.get("max_retries",    3))
    otp_timeout     = getattr(args, "timeout",          None) or int(cfg.get("otp_wait_secs", 120))
    circuit_breaker = getattr(args, "circuit_breaker",  None) or int(cfg.get("circuit_breaker", 3))

    cfg.update({
        "enabled":              True,
        "webhook_url":          webhook_url,
        "ntfy_topic":           ntfy_topic,
        "max_retries":          max_retries,
        "otp_wait_secs":        otp_timeout,
        "circuit_breaker":      circuit_breaker,
        "consecutive_failures": 0,
        "circuit_tripped":      False,
    })
    _save_auto_login_cfg(cfg)
    cprint("[bold green]✓ 自动登录已启用[/bold green]")

    # 打印 iPhone 快捷指令配置指南
    if webhook_url:
        otp_post_url = f"{webhook_url}/otp"
        console.print(Panel(
            f"[bold]iPhone 快捷指令配置[/bold]\n\n"
            f"  1. 自动化 → 收到含「验证码」的短信时触发\n"
            f"  2. 获取文本 → 将快捷指令输入转为纯文本\n"
            f"  3. 获取URL内容:\n"
            f"       POST  [cyan]{otp_post_url}[/cyan]\n"
            f"       请求体 = [yellow]整条短信原文[/yellow]\n\n"
            f"[dim]macli 自动从短信中提取 6 位验证码，手机端无需做正则。[/dim]\n\n"
            f"[bold]验证：[/bold]curl -X POST {otp_post_url} -d '123456'",
            title="[bold]自动登录 — Webhook 配置[/bold]",
            border_style="cyan",
            padding=(1, 2),
        ))
    else:
        ntfy_url = f"https://ntfy.sh/{ntfy_topic}"
        ntfy_publish_url = "https://ntfy.sh"
        console.print(Panel(
            f"[bold]iPhone 快捷指令配置（推荐：整条短信原文直传）[/bold]\n\n"
            f"[bold cyan]推荐 URL[/bold cyan]\n"
            f"  {ntfy_url}\n\n"
            f"[bold cyan]方法[/bold cyan]\n"
            f"  POST\n\n"
            f"[bold cyan]Headers（可选）[/bold cyan]\n"
            f"  Content-Type  →  text/plain; charset=utf-8\n\n"
            f"[bold cyan]请求体[/bold cyan]\n"
            f"  [yellow]<整条短信原文 / 转成纯文本后的快捷指令输入>[/yellow]\n\n"
            f"[dim]macli 会在收到的消息正文里自动提取首个 6 位数字验证码，无需手机端先做正则。[/dim]\n\n"
            f"---\n\n"
            f"[bold]建议快捷指令流程：[/bold]\n"
            f"  1. 触发条件：收到含「验证码」的短信\n"
            f"  2. 用 [获取文本] 把「快捷指令输入」转换成纯文本\n"
            f"  3. [获取URL内容] → POST {ntfy_url}\n"
            f"     Header: Content-Type = text/plain; charset=utf-8\n"
            f"     请求体: [yellow]<上一步得到的整条短信文本>[/yellow]\n\n"
            f"[bold]备用方案（若你更想发 JSON）[/bold]\n"
            f"  URL: {ntfy_publish_url}\n"
            f"  Header: Content-Type = application/json\n"
            f"  JSON 请求体: topic = [cyan]{ntfy_topic}[/cyan]\n"
            f"                 message = [yellow]<整条短信文本>[/yellow]",
            title="[bold]自动登录 — 手机快捷指令配置指南[/bold]",
            border_style="cyan",
            padding=(1, 2),
        ))
        cprint(f"\n[dim]配置已保存。ntfy topic 请妥善保管（泄露后他人可读取验证码）[/dim]")


def _main_impl():
    p = argparse.ArgumentParser(
        prog="modelarts",
        description="华为云 ModelArts CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
macli login  # cli版登录，适用于大多数环境
macli login --cookie <COOKIE_STRING>  # 直接使用cookie字符串登录

macli whoami [--json]
macli logout  # 退出登录，清除保存在 config/ 目录中的登录凭据

macli region list [--json]  # 列出可用的region列表
macli region select  # 交互式选择当前region
macli region select --name <REGION_NAME>  # 切换区域，例如 REGION_NAME cn-north-9 为 华北-乌兰察布一

macli workspace list [--json]  # 列出可用的workspace列表
macli workspace select  # 交互式选择当前workspace
macli workspace select --name <WORKSPACE_NAME>
macli workspace select --id <WORKSPACE_ID>

macli jobs [filters...] [--limit LIMIT] [--json]  # 列出作业列表，支持多种过滤条件（默认显示 SSH 端口）
macli jobs [filters...] [--refresh] [--json]      # --refresh 清空端口缓存并重新拉取
macli jobs count [filters...] [--json] # 仅返回满足过滤条件的作业数量
macli ports [--refresh] [--json]       # 查看当前 Running 作业的 SSH 端口

# jobs filters:
#   [--name <NAME>]
#   [--recent <DURATION>]  # e.g. 1h, 30m, 2d, 1y, etc.  m means months, y means years
#   [--running] [--failed] [--terminated] [--pending] | [--status STATUS [STATUS ...]]
#   [--gpu-count N [N ...]]

macli detail <JOB_ID> [--json]
macli detail --name <JOB_NAME> [--json]

macli events <JOB_ID> [--limit LIMIT] [--offset OFFSET] [--json]
macli log <JOB_ID> --output <OUTPUT_PATH> [--task TASK]
macli usage [<JOB_ID>] [--minutes N] [--step N] [--probe [--task TASK]] [--json]
macli jobs [filters...]              # 默认显示 SSH 端口（Running 作业自动缓存）
macli jobs [filters...] --refresh   # 清空端口缓存并重新拉取
macli shell <JOB_ID> [--task TASK]
macli ssh <JOB_ID> [--task TASK] [--identityfile PATH/NAME]
macli identityfile add <PATH> [--name/-n <NAME>]
macli identityfile remove <NAME>
macli identityfile list
macli identityfile default [--set <PATH/NAME>]
macli exec <JOB_ID> -- <cmd> [args...]
macli exec <JOB_ID> --script <file> [--cwd <dir>]
macli exec <JOB_ID> --stdin [--cwd <dir>]

macli copy <JOB_ID> [options...] [-y | --yes] [--json]
macli copy --src-name <SRC_NAME> [options...] [-y | --yes] [--json]

# copy options:
#   [--gpu-count N]  # 覆盖原有的GPU数量
#   [--name NEW_NAME]  # 新拷贝的作业名称
#   [--desc NEW_DESC]  # 新拷贝的作业描述
#   [--command COMMAND | --command-file COMMAND_FILE]  # 覆盖新拷贝的启动命令

macli stop <JOB_ID> [-y | --yes]

macli delete <JOB_ID> [-y | --yes] [-f | --force]  # -f/--force 会强制删除正在运行的作业, -y/--yes 会跳过删除确认提示
""")
    p.add_argument("--debug", dest="debug", action="store_true",
                   help="调试模式：输出详细内部信息")
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("login", help="登录")
    q.add_argument("--cookie",      dest="cookie",      default=None,
                   help="直接传入 cookie 字符串，跳过交互登录")
    q.add_argument("--interactive", dest="interactive", action="store_true",
                   help="登录后交互式选择 region 和 workspace")

    q = sub.add_parser("logout", help="清除登录 session（keyring 凭据默认保留，--purge 可一并删除）")
    q.add_argument("--purge", action="store_true", help="同时清除 keyring 中保存的账号密码")

    q = sub.add_parser("autologin", help="管理会话过期时的自动重新登录")
    q.add_argument("action", nargs="?", choices=["enable", "disable", "status"],
                   default="status", help="操作：status（默认）/ enable / disable")
    q.add_argument("--retries",          type=int, default=None, metavar="N",
                   help="最大重试次数，默认 3")
    q.add_argument("--timeout",          type=int, default=None, metavar="SECS",
                   help="每次等待手机验证码的超时秒数，默认 120")
    q.add_argument("--circuit-breaker",  dest="circuit_breaker", type=int, default=None, metavar="N",
                   help="连续失败 N 次后自动禁用 autologin（熔断），默认 3")
    q.add_argument("--reset-topic",      dest="reset_topic", action="store_true",
                   help="重新生成 ntfy topic（原快捷指令配置将失效）")

    q = sub.add_parser("server", help="管理 GPU 状态 HTTP 服务（/gpu /log /server-log）")
    q.add_argument("server_action", nargs="?",
                   choices=["enable", "disable", "status", "run"],
                   default="status",
                   help="操作：status（默认）/ enable / disable / run")
    q.add_argument("--port", type=int, default=None, metavar="PORT",
                   help="监听端口，默认 8086")

    q = sub.add_parser("watch", help="管理定时检查任务（保活 + 清理终止作业）")
    q.add_argument("watch_action", nargs="?",
                   choices=["enable", "disable", "status", "run"],
                   default="status",
                   help="操作：status（默认）/ enable / disable / run")
    q.add_argument("--interval", type=float, default=1.0, metavar="H",
                   help="检查间隔（小时），仅 enable 时生效，默认 1")
    q.add_argument("--script",   default=None, metavar="PATH",
                   help="check_jobs.py 的路径（enable/run 时使用）")
    q.add_argument("--threshold-hours", dest="threshold_hours", type=int, default=72,
                   metavar="N", help="Terminated 作业保留时长（小时），默认 72")

    q = sub.add_parser("whoami", help="显示当前登录状态")
    q.add_argument("--json", action="store_true", help="JSON 输出")

    # region 子命令
    rg = sub.add_parser("region", help="区域管理").add_subparsers(dest="rg_cmd", required=True)
    q = rg.add_parser("list", help="列出所有可用区域")
    q.add_argument("--json", action="store_true", help="JSON 输出")
    q = rg.add_parser("select", help="切换区域")
    q.add_argument("--name", default=None, help="区域名称，如 cn-north-9")

    # workspace 子命令
    ws = sub.add_parser("workspace", help="工作空间管理").add_subparsers(dest="ws_cmd", required=True)
    q = ws.add_parser("list", help="列出所有工作空间")
    q.add_argument("--json", action="store_true", help="JSON 输出")
    q = ws.add_parser("select", help="切换工作空间")
    q.add_argument("--id",   dest="id",   default=None, help="按 ID 选择")
    q.add_argument("--name", dest="name", default=None, help="按名称选择")

    q = sub.add_parser("jobs", help="列出训练作业")
    q.add_argument("action",      nargs="?", choices=["count"], default=None,
                   help="子操作：count 只输出数量")
    q.add_argument("--limit",      type=int, default=50)
    q.add_argument("--recent",     default=None,
                   metavar="DURATION", help="最近 N 时间内创建的，如 4d/5h/3m/1y")
    q.add_argument("--running",    action="store_true", help="只显示运行中")
    q.add_argument("--failed",     action="store_true", help="只显示失败")
    q.add_argument("--terminated", action="store_true", help="只显示已终止")
    q.add_argument("--pending",    action="store_true", help="只显示排队中")
    q.add_argument("--status",     dest="status", nargs="+", default=None,
                   metavar="STATUS",
                   help="按状态过滤，支持多选，可用值: running failed terminated pending "
                        "或原始值如 Running/Failed/Stopped 等")
    q.add_argument("--gpu-count",  dest="gpu_count", type=int, nargs="+",
                   metavar="N",    help="按 GPU 卡数过滤，支持多选")
    q.add_argument("--name",       dest="name",      default=None,
                   metavar="NAME", help="按作业名称精确过滤")
    q.add_argument("--refresh",    action="store_true",
                   help="清空 SSH 端口缓存，强制重新拉取所有 Running 作业的端口信息")
    q.add_argument("--json",       action="store_true", help="JSON 输出")

    q = sub.add_parser("query", help="按条件筛选作业，输出 ID（可管道给 stop/delete/exec 等）")
    q.add_argument("--limit",      type=int, default=500,
                   metavar="N",    help="最多返回多少个作业（默认 500）")
    q.add_argument("--recent",     default=None,
                   metavar="DURATION", help="最近 N 时间内创建的，如 4d/5h/3m/1y")
    q.add_argument("--running",    action="store_true", help="只选 Running 作业")
    q.add_argument("--failed",     action="store_true", help="只选 Failed 作业")
    q.add_argument("--terminated", action="store_true", help="只选已终止的作业")
    q.add_argument("--pending",    action="store_true", help="只选排队中的作业")
    q.add_argument("--status",     dest="status", nargs="+", default=None,
                   metavar="STATUS", help="按状态过滤（同 jobs --status）")
    q.add_argument("--gpu-count",  dest="gpu_count", type=int, nargs="+",
                   metavar="N",    help="按 GPU 卡数过滤，支持多选")
    q.add_argument("--name",       dest="name", default=None,
                   metavar="NAME", help="按作业名称精确过滤")
    q.add_argument("--json",       action="store_true",
                   help="JSON 输出（含 id/name/status）")

    q = sub.add_parser("ports", help="查看当前 Running 作业的 SSH 端口")
    q.add_argument("--refresh", action="store_true",
                   help="清空 SSH 端口缓存，强制重新拉取所有 Running 作业的端口信息")
    q.add_argument("--json", action="store_true", help="JSON 输出（list）")

    q = sub.add_parser("detail", help="查看作业详情及 SSH 信息（直接调用 API）")
    grp = q.add_mutually_exclusive_group(required=True)
    grp.add_argument("job_id",     metavar="JOB_ID",  nargs="?", default=None,
                     help="作业 ID")
    grp.add_argument("--name",     dest="src_name",   default=None,
                     help="按作业名称查找（取最新一个）")
    q.add_argument("--json", action="store_true", help="JSON 输出")

    q = sub.add_parser("events", help="作业事件详情")
    q.add_argument("job_id", metavar="JOB_ID", help="作业 ID")
    q.add_argument("--limit", type=int, default=50, help="返回事件条数上限")
    q.add_argument("--offset", type=int, default=0, help="结果偏移量")
    q.add_argument("--json", action="store_true", help="JSON 输出")

    q = sub.add_parser("log", help="下载完整日志文件")
    q.add_argument("job_id", metavar="JOB_ID", help="作业 ID")
    q.add_argument("--task", default=None, help="任务名，例如 worker-0；默认自动选择")
    q.add_argument("--timeout", type=int, default=120, help="下载日志文件时的超时时间")
    q.add_argument("--output", required=True, help="输出文件路径")
    q.add_argument("--json", action="store_true", help="仅输出解析到的任务信息（调试用）")

    q = sub.add_parser("usage", help="查看作业资源使用监控")
    q.add_argument("job_id", metavar="JOB_ID", nargs="?", default=None, help="作业 ID；不填则列出所有 Running 作业最近 usage")
    q.add_argument("--minutes", type=int, default=15, help="最近多少分钟，默认 15")
    q.add_argument("--step", type=int, default=60, help="采样步长（秒），默认 60")
    q.add_argument("--limit", type=int, default=50, help="无 JOB_ID 时，最多检查多少个作业，默认 50")
    q.add_argument("--concurrency", "-c", type=int, default=8,
                   metavar="N", help="无 JOB_ID 时的并发采集数，默认 8")
    q.add_argument("--metrics", "-m", nargs="+", metavar="METRIC",
                   help="只显示指定指标，可多选：cpu mem gpu vram（默认全部）")
    q.add_argument("--probe", action="store_true",
                   help="通过 CloudShell exec 直接从容器内采集指标（不走监控 API）")
    q.add_argument("--task", default=None, help="--probe 时指定任务名，例如 worker-0；默认自动选第一个")
    q.add_argument("--timeout", type=int, default=60,
                   help="--probe 模式下的采集超时秒数，默认 60")
    q.add_argument("--json", action="store_true", help="JSON 输出")

    q = sub.add_parser("shell", help="打开作业 CloudShell 交互终端")
    q.add_argument("job_id", metavar="JOB_ID", help="作业 ID")
    q.add_argument("--task", default=None, help="任务名，例如 worker-0；默认自动选择")
    q.add_argument("--heartbeat", type=float, default=2.0, help="空闲时发送心跳包的间隔秒数，默认 2")

    q = sub.add_parser("ssh", help="通过原生 SSH 连接作业容器")
    q.add_argument("job_id", metavar="JOB_ID", help="作业 ID")
    q.add_argument("--task", default=None, help="任务名，例如 worker-0；默认自动选择")
    q.add_argument("--identityfile", "-i", dest="identityfile", default=None,
                   metavar="PATH/NAME", help="SSH 私钥路径或已保存密钥名称；不指定则使用默认密钥")
    q.add_argument("--opt", "-o", dest="ssh_opts", action="append", default=None,
                   metavar="SSH_OPTION", help="追加额外 SSH 选项，例如 -o StrictHostKeyChecking=no（可多次使用）")

    # identityfile 子命令
    _if = sub.add_parser("identityfile", help="管理 SSH 密钥配置").add_subparsers(dest="if_cmd", required=True)
    q = _if.add_parser("add", help="添加 SSH 密钥")
    q.add_argument("path", metavar="PATH", help="密钥文件路径")
    q.add_argument("--name", "-n", default=None, help="可选别名；不指定则使用文件名（不含扩展名）")
    q = _if.add_parser("remove", help="移除已保存的 SSH 密钥")
    q.add_argument("name", metavar="NAME", help="密钥名称")
    _if.add_parser("list", help="列出所有已保存的 SSH 密钥")
    q = _if.add_parser("default", help="查看或设置默认 SSH 密钥")
    q.add_argument("--set", default=None, metavar="PATH/NAME", help="设置默认密钥（路径或已保存名称）")

    q = sub.add_parser("exec", help="在作业容器内执行命令并输出结果",
                       formatter_class=argparse.RawDescriptionHelpFormatter,
                       epilog="""
示例:
  macli exec JOB_ID -- echo hello
  macli exec JOB_ID -- ls -la /cache
  macli exec JOB_ID --cwd /workspace -- python train.py
  macli exec JOB_ID --script run.sh
  cat script.sh | macli exec JOB_ID --stdin
  macli exec JOB_ID --stdin << 'EOF'
  for f in /cache/*.log; do echo "$f"; done
  EOF

  # 切换到 SSH 后端（记忆，后续无需再加 --backend）
  macli exec JOB_ID --backend ssh -- nvidia-smi
  macli exec JOB_ID --backend cloudshell -- nvidia-smi
  macli exec --backend ssh        # 仅切换默认后端，不执行命令
""")
    q.add_argument("job_id", metavar="JOB_ID", nargs="?", default=None,
                   help="作业 ID；省略时仅保存 --backend 设置")
    q.add_argument("--task",    default=None, help="任务名，例如 worker-0；默认自动选择")
    q.add_argument("--cwd",     default=None, metavar="DIR", help="执行命令前先切换到指定目录")
    q.add_argument("--timeout", type=int, default=300, help="等待命令结束的超时秒数，默认 300")
    q.add_argument("--backend", choices=["cloudshell", "ssh"], default=None,
                   help="执行后端：cloudshell（默认）或 ssh；指定后自动记忆，下次无需重复")
    q.add_argument("--identityfile", "-i", dest="identityfile", default=None,
                   metavar="PATH/NAME", help="SSH 后端：私钥路径或已保存名称；不指定则使用默认密钥")
    q.add_argument("--opt", "-o", dest="ssh_opts", action="append", default=None,
                   metavar="SSH_OPTION", help="SSH 后端：追加额外 SSH 选项（可多次使用）")
    src = q.add_mutually_exclusive_group()
    src.add_argument("--script", dest="script_file", default=None, metavar="FILE",
                     help="从本地文件读取要执行的脚本")
    src.add_argument("--stdin",  dest="use_stdin",   action="store_true",
                     help="从 stdin 读取要执行的脚本")
    q.add_argument("inline_cmd", metavar="CMD", nargs="*",
                   help="-- 后接的命令及参数（简单命令；复杂脚本请用 --stdin/--script）")

    q = sub.add_parser("copy", help="复制训练作业",
                       formatter_class=argparse.RawDescriptionHelpFormatter,
                       epilog="""
示例:
  macli copy xxxx --yes
  macli copy xxxx --gpu-count 2 --yes
  macli copy xxxx --name my-exp --desc "4卡实验" --gpu-count 4 --yes
  macli copy xxxx --command "mkdir /cache\\nsleep 2000000000s;" --yes
  macli copy xxxx --command-file start.sh --yes
""")
    grp = q.add_mutually_exclusive_group(required=True)
    grp.add_argument("job_id",    metavar="JOB_ID", nargs="?", default=None,
                     help="源作业 ID")
    grp.add_argument("--src-name", dest="src_name", default=None,
                     help="按源作业名称指定（取最新一个）")
    q.add_argument("--gpu-count", dest="gpu_count",   type=int, default=None,
                   help="GPU卡数，可选 1/2/4/8，默认保持原规格")
    q.add_argument("--name",      dest="name",        default=None,
                   help="新作业名称，默认自动生成 <原名>-copy-XXXXX")
    q.add_argument("--desc",      dest="description", default=None,
                   help="作业描述（可选，不指定则保留原描述）")
    q.add_argument("--command",      dest="command",      default=None,
                   help=r"启动命令，换行用 \n，默认保持原命令")
    q.add_argument("--command-file", dest="command_file", default=None,
                   help="从文件读取启动命令，与 --command 互斥")
    q.add_argument("-y", "--yes", action="store_true",
                   help="跳过确认直接提交")
    q.add_argument("--json", action="store_true", help="创建成功后 JSON 输出新作业信息")

    q = sub.add_parser("stop", help="终止训练作业（支持多个 ID 或管道输入）")
    q.add_argument("job_ids", metavar="JOB_ID", nargs="*",
                   help="一个或多个 JOB_ID；不提供时从 stdin 读取（管道用法）")
    q.add_argument("-y", "--yes", action="store_true")

    q = sub.add_parser("delete", help="删除训练作业（支持多个 ID 或管道输入）")
    q.add_argument("job_ids", metavar="JOB_ID", nargs="*",
                   help="一个或多个 JOB_ID；不提供时从 stdin 读取（管道用法）")
    q.add_argument("-f", "--force", action="store_true", help="强制删除（包括运行中的作业）")
    q.add_argument("-y", "--yes",   action="store_true")

    # parse_known_args：让子解析器先处理已知参数，remaining 用于处理
    # "exec JOB_ID -- cmd args" 中 -- 被 argparse 顶层提前消费的已知 bug
    args, _remaining = p.parse_known_args()
    if _remaining:
        if getattr(args, "cmd", None) == "exec":
            # 合并剩余参数（去掉前导 --）
            extra = _remaining[1:] if _remaining[0] == "--" else _remaining
            args.inline_cmd = list(getattr(args, "inline_cmd", None) or []) + extra
        else:
            p.error(f"unrecognized arguments: {' '.join(_remaining)}")

    # --command 中支持字面 \n 转换为真正换行
    if hasattr(args, "command") and args.command:
        args.command = args.command.replace("\\n", "\n")

    global _VERBOSE
    _VERBOSE = getattr(args, "debug", False)

    if args.cmd == "region":
        {"list": cmd_region_list, "select": cmd_region_select}[args.rg_cmd](args)
    elif args.cmd == "workspace":
        {"list": cmd_workspace_list, "select": cmd_workspace_select}[args.ws_cmd](args)
    elif args.cmd == "identityfile":
        cmd_identityfile(args)
    else:
        {"login":        cmd_login,
         "logout":       cmd_logout,
         "autologin":    cmd_autologin,
         "server":       cmd_server,
         "watch":        cmd_watch,
         "whoami":       cmd_whoami,
         "jobs":         cmd_list_jobs,
         "query":        cmd_query,
         "ports":        cmd_ports,
         "detail":       cmd_detail,
         "events":       cmd_events,
         "log":          cmd_log,
         "usage":        cmd_usage,
         "shell":        cmd_shell,
         "ssh":          cmd_ssh,
         "exec":         cmd_exec,
         "copy":         cmd_copy,
         "stop":         cmd_stop,
         "delete":       cmd_delete}[args.cmd](args)


def main():
    _init_logger()
    _flog("INFO", "macli " + " ".join(sys.argv[1:]))
    exit_code = 0
    try:
        try:
            result = _main_impl()
            exit_code = result if isinstance(result, int) else 0
        except SessionExpiredError as e:
            cfg = _load_auto_login_cfg()
            if cfg.get("enabled"):
                dprint(f"[dim]SessionExpiredError: {e}，触发自动登录[/dim]")
                ok = _do_auto_login(cfg)
                if ok:
                    _autologin_record_outcome(True)
                    dprint(f"[dim]重新执行: {sys.argv}[/dim]")
                    _flog("INFO", "会话已过期，自动重登成功，重新执行")
                    os.execvp(sys.argv[0], sys.argv)
                    return 0
                tripped = _autologin_record_outcome(False)
                if tripped:
                    n = int(cfg.get("circuit_breaker", 3))
                    console.print(
                        f"[bold red]✗ 熔断：自动登录连续失败 {n} 次，autologin 已自动禁用[/bold red]"
                    )
                    console.print(
                        "[yellow]请检查 ntfy/网络配置，再执行 [bold]macli autologin enable[/bold] 重新启用[/yellow]"
                    )
                else:
                    console.print("[bold red]✗ 自动重新登录失败[/bold red]，请手动执行 [bold]macli login[/bold]")
                exit_code = 2
            else:
                console.print(f"\n[bold red]✗ 登录已过期[/bold red]  {e}")
                console.print("[yellow]请重新执行：[/yellow] [bold]macli login[/bold]")
                exit_code = 2
        _flog("INFO" if exit_code == 0 else "ERROR", f"exit {exit_code}")
        return exit_code
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        _flog("INFO" if code == 0 else "ERROR", f"exit {code}")
        raise
    except Exception as e:
        _flog("ERROR", f"exit 1 - {type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    sys.exit(main())
