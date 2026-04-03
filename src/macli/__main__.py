#!/usr/bin/env python3
"""
华为云 ModelArts 远程管理 CLI
author: @hammershock
version: 0.0.1
"""
import os, sys, json, time, re, copy, argparse, subprocess as _subprocess, tempfile, shutil, ssl, socket, base64, struct, urllib.parse, threading, tty, termios, select, signal
from pathlib import Path
from datetime import datetime


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

def cprint(msg, style=None):
    console.print(msg, style=style)

def dprint(msg, style=None):
    """仅在 --debug 模式下输出"""
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


# ── 凭据安全存储（系统 Keychain）────────────────────────────────

def _load_saved_creds() -> dict:
    """从系统 Keychain 读取已保存的账号密码，返回 {"domain", "username", "password"} 或 {}"""
    if not _KEYRING_OK:
        return {}
    try:
        raw = _keyring.get_password(_KR_SERVICE, _KR_KEY)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {}


def _save_creds(domain: str, username: str, password: str) -> bool:
    """将账号密码加密存入系统 Keychain，成功返回 True"""
    if not _KEYRING_OK:
        return False
    try:
        _keyring.set_password(
            _KR_SERVICE, _KR_KEY,
            json.dumps({"domain": domain, "username": username, "password": password},
                       ensure_ascii=False)
        )
        return True
    except Exception:
        return False


def _clear_saved_creds() -> bool:
    """从系统 Keychain 删除已保存的账号密码，成功返回 True"""
    if not _KEYRING_OK:
        return False
    try:
        _keyring.delete_password(_KR_SERVICE, _KR_KEY)
        return True
    except Exception:
        return False


def load_detail_cache() -> dict:
    """从 session 的 detail_cache 字段读取 job detail 缓存。
    返回 {job_id: job_detail_dict, ...}。
    """
    d = load_session()
    return d.get("detail_cache", {})


def save_detail_cache(cache: dict):
    """将 detail cache 写回 session 的 detail_cache 字段。"""
    d = load_session()
    d["detail_cache"] = cache
    save_session(d)


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
    try: return datetime.fromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d %H:%M")
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


def ssh_ports_summary(entries: list) -> str:
    """用于 jobs 表格展示 SSH 端口。多个端口去重后以逗号连接；缺失返回 —。"""
    ports = []
    for item in entries or []:
        port = item.get("port")
        if port is None:
            port = ssh_url_to_port(item.get("url", ""))
        if port is not None:
            ports.append(str(port))
    ports = list(dict.fromkeys(ports))
    return ",".join(ports) if ports else "—"

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
        save_session({
            "region":       region,
            "project_id":   project_id,
            "agency_id":    agency_id,
            "workspace_id": workspace_id,
            "cftk":         cftk,
            "cookies":      {c.name: c.value for c in self.http.cookies},
            "cookie_str":   cookie_str,
            "saved_at":     time.time(),
        })

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
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]列表失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job(self, job_id: str) -> dict:
        r = self.sess.get(f"/training-jobs/{job_id}")
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
        if r.status_code == 200:
            data = self._safe_json(r)
            return data if isinstance(data, list) else []
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
        if r.status_code in (200, 201):
            return self._safe_json(r)
        cprint(f"[red]创建失败 {r.status_code}: {r.text[:400]}[/red]")
        return {}

    def delete_job(self, job_id: str) -> bool:
        r = self.sess.http.delete(
            f"{self.sess.base}/training-jobs/{job_id}",
            json={}, timeout=15)
        if r.status_code in (200, 202):
            return True
        cprint(f"[red]删除失败 {r.status_code}: {r.text[:200]}[/red]")
        return False

    def stop_job(self, job_id: str) -> bool:
        r = self.sess.post(
            f"/training-jobs/{job_id}/actions",
            {"action_type": "terminate"})
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
    if r.status_code == 200:
        return r.json()
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
                service: str = "https://console.huaweicloud.com/console/") -> str:
    """
    纯 HTTP 登录华为云（IAM 用户 + 短信 MFA），返回 cookie 字符串。
    失败返回空字符串。
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
        return ""
    if data.get("loginResult") != "success":
        cprint(f"[red]密码登录失败: {data.get('loginResult')} — {data.get('loginMessage','')}[/red]")
        return ""
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
        return ""
    if anti.get("result") != "success":
        cprint(f"[red]MFA 信息获取失败: {anti}[/red]")
        return ""
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
    sys.stdout.flush()
    sms_code = input("\n请输入收到的 6 位验证码: ").strip()
    if not sms_code:
        cprint("[red]验证码不能为空[/red]")
        return ""

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
        return ""
    location = r.headers.get("location", "")
    if "actionErrors=419" in location:
        cprint("[red]验证码无效或已失效（419），请重试[/red]")
        return ""

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
        return ""

    dprint(f"[dim]获取到 {len(all_cookies)} 个 cookie，共 {len(ck)} 字符[/dim]")
    return ck


def _extract_cftk(cookie_str: str) -> str:
    """从 cookie 字符串中提取 cftk 值"""
    for part in cookie_str.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == "cftk":
            return v.strip()
    return ""


def _get_cookie_from_args_or_input(args) -> str:
    """按优先级获取 cookie：--cookie 参数 > session 缓存 > Keychain 自动登录 > 交互登录"""
    # 1. --cookie 参数直接给了
    if getattr(args, "cookie", None):
        ck = args.cookie.strip()
        dprint(f"[green]✓ 使用 --cookie 参数（{len(ck)} 字符）[/green]")
        return ck

    # 2. 从已保存的 session 中读取 cookie
    d = load_session()
    ck = d.get("cookie_str", "")
    if ck:
        dprint(f"[green]✓ 从 session 读取 cookie（{len(ck)} 字符）[/green]")
        return ck

    # 3. 从系统 Keychain 读取已保存的账号密码，自动登录
    saved = _load_saved_creds()
    if saved.get("domain") and saved.get("username") and saved.get("password"):
        cprint(f"[cyan]使用已保存的账号自动登录：[bold]{saved['username']}[/bold] @ {saved['domain']}[/cyan]")
        ck = _http_login(saved["domain"], saved["username"], saved["password"])
        if ck:
            dprint("[green]✓ Keychain 自动登录成功[/green]")
            return ck
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
        return ""

    ck = _http_login(_domain, _username, _password)
    if not ck:
        return ""

    # 登录成功后询问是否保存账号密码到系统 Keychain
    if _KEYRING_OK:
        try:
            save_it = input("记住账号密码，下次自动登录？[y/N] ").strip().lower()
        except EOFError:
            save_it = "n"
        if save_it == "y":
            if _save_creds(_domain, _username, _password):
                cprint("[green]✓ 账号密码已安全保存至系统 Keychain[/green]")
            else:
                cprint("[yellow]保存失败，本次密码不会被记住[/yellow]")
    else:
        dprint("[dim]keyring 不可用，跳过密码保存[/dim]")

    return ck


def _setup_session_from_cookie(ck: str, interactive: bool) -> None:
    """用 cookie 初始化 session，interactive=True 时交互选择 region/workspace"""
    cftk = _extract_cftk(ck)
    dprint("[green]✓ 从 cookie 中提取 cftk[/green]")

    http = _new_session()
    for part in ck.split(";"):
        k, _, v = part.strip().partition("=")
        if k: http.cookies.set(k.strip(), v.strip())

    # 拿 me 信息（用 cn-north-9 作为初始 region）
    dprint("[cyan]验证 cookie 并获取账号信息...[/cyan]")
    me = _me(http, "cn-north-9", cftk)
    support_regions = me.get("supportRegions", [])
    if not support_regions:
        cprint("[red]无法获取账号信息，请确认 cookie 有效[/red]")
        sys.exit(1)

    if interactive:
        # 交互式选择 region
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
        # 默认取 me 返回的当前 region
        region = me.get("region", "cn-north-9")
        dprint(f"[cyan]使用默认区域: {region}  {REGION_NAMES.get(region,'')}[/cyan]")

    # 获取该 region 的 project_id
    dprint(f"[cyan]获取 {region} 的 project_id...[/cyan]")
    me2       = _me(http, region, cftk)
    project_id = me2.get("projectId", "")
    agency_id  = me2.get("id") or me2.get("userId", "")
    if not project_id:
        cprint(f"[red]无法获取 {region} 的 project_id[/red]"); sys.exit(1)
    dprint(f"[green]✓ region={region}  project_id={project_id}[/green]")

    sess = ConsoleSession()
    sess.init(ck, region, project_id, agency_id, cftk, "")
    # 同时把原始 cookie 字符串存入 session，供下次 login 复用
    d = load_session(); d["cookie_str"] = ck; save_session(d)

    if interactive:
        wsid = _select_workspace(sess)
        sess.workspace_id = wsid
        d = load_session(); d["workspace_id"] = wsid; save_session(d)
    else:
        # 非交互模式：自动选第一个工作空间作为默认
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
    ck = _get_cookie_from_args_or_input(args)

    # 登录失败时，只有 --cookie 路径才继续
    if not ck:
        if not getattr(args, "cookie", None):
            # 没传 --cookie，登录失败，直接退出
            sys.exit(1)
        # --cookie 传了但为空，展示指南让用户粘贴
        ck = _manual_cookie_input()
        if not ck:
            sys.exit(1)

    # 验证 cftk，失败时展示指南让用户重新粘贴
    if not _extract_cftk(ck):
        cprint("[red]cookie 无效（无法提取 cftk），请重新获取[/red]")
        ck = _manual_cookie_input()
        if not ck:
            sys.exit(1)
        if not _extract_cftk(ck):
            cprint("[red]cookie 仍然无效，退出[/red]")
            sys.exit(1)

    _setup_session_from_cookie(ck, interactive=getattr(args, "interactive", False))
    cprint(f"\n[green]✓ 登录成功！[/green]")
    if not getattr(args, "interactive", False):
        dprint("[dim]提示：使用 region select / workspace select 配置区域和工作空间[/dim]")


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
    """清除已保存的登录凭据"""
    p = _config_path()
    cleared_session = False
    if p.exists():
        p.unlink()
        cleared_session = True

    cleared_creds = _clear_saved_creds()

    if cleared_session or cleared_creds:
        parts = []
        if cleared_session: parts.append("session")
        if cleared_creds:   parts.append("Keychain 账号密码")
        cprint(f"[green]✓ 已清除：{' 及 '.join(parts)}[/green]")
    else:
        cprint("[yellow]当前没有已保存的登录凭据[/yellow]")


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
        f"{datetime.fromtimestamp(d.get('saved_at',0)).strftime('%Y-%m-%d %H:%M')}"
        f"  ({age:.1f}h 前)",
        title="Session 状态", border_style="green"))
    
def cmd_list_jobs(args):
    sess = _sess_or_exit()
    api  = API(sess)

    # 所有过滤在本地完成，分页拉取（每页最多 50，API 硬限制）
    PAGE = 50
    need_filter = bool(args.recent or args.running or args.failed
                       or args.terminated or args.pending or args.gpu_count
                       or args.name or args.status)
    jobs  = []
    total = 0
    offset = 0
    while True:
        data   = api.list_jobs(limit=PAGE, offset=offset)
        total  = data.get("total", 0)
        page   = data.get("items", [])
        jobs  += page
        offset += len(page)
        # 如果不需要过滤，拉够 limit 条就停
        if not need_filter and len(jobs) >= args.limit:
            break
        # 没有更多了
        if not page or offset >= total:
            break
        # 已经拉了足够多了（最多拉 500 条，避免无限循环）
        if offset >= 500:
            break

    # ── 时间过滤 ──
    if args.recent:
        try:
            delta_ms = parse_recent(args.recent)
        except ValueError as e:
            cprint(f"[red]{e}[/red]"); sys.exit(1)
        cutoff = int(time.time() * 1000) - delta_ms
        jobs = [j for j in jobs
                if (j.get("metadata", {}).get("create_time") or 0) >= cutoff]

    # ── 状态过滤 ──
    # --running/--failed/--terminated/--pending 和 --status 合并
    STATUS_ALIAS = {
        "running":    {"Running"},
        "failed":     {"Failed"},
        "terminated": {"Stopped", "Terminated"},
        "pending":    {"Pending", "Waiting"},
    }
    status_filter = set()
    if args.running:    status_filter |= STATUS_ALIAS["running"]
    if args.failed:     status_filter |= STATUS_ALIAS["failed"]
    if args.terminated: status_filter |= STATUS_ALIAS["terminated"]
    if args.pending:    status_filter |= STATUS_ALIAS["pending"]
    for s in (args.status or []):
        status_filter |= STATUS_ALIAS.get(s.lower(), {s})
    if status_filter:
        jobs = [j for j in jobs
                if j.get("status", {}).get("phase", "") in status_filter]

    # ── GPU 卡数过滤（多选）──
    if args.gpu_count:
        allowed = set(args.gpu_count)
        jobs = [j for j in jobs
                if j.get("spec", {}).get("resource", {})
                    .get("pool_info", {}).get("accelerator_num") in allowed]

    # ── 名称过滤 ──
    if args.name:
        jobs = [j for j in jobs
                if j.get("metadata", {}).get("name", "") == args.name]

    # 截取到 limit
    jobs = jobs[:args.limit]

    # ── count 模式：只输出数量 ──
    if getattr(args, "action", None) == "count":
        if getattr(args, "json", False):
            _json_out({"count": len(jobs)})
        else:
            print(len(jobs))
        return

    if getattr(args, "detail", False) and jobs:
        do_refresh = getattr(args, "refresh", False)

        # 缓存结构：{job_id: endpoints_dict}
        # 只缓存 detail 接口独有的 endpoints 字段（SSH 等）
        # job 的状态、时长等会变化的字段始终来自列表接口的新鲜数据，不参与缓存
        cache = {} if do_refresh else load_detail_cache()
        if do_refresh:
            dprint("[dim]--refresh: 已清空 detail 缓存，强制重新拉取[/dim]")

        # 清理缓存中已不存在于本次可见列表的 job ID
        current_ids = {j.get("metadata", {}).get("id", "") for j in jobs if j.get("metadata", {}).get("id")}
        stale_ids = [k for k in list(cache.keys()) if k not in current_ids]
        if stale_ids:
            for k in stale_ids:
                del cache[k]
            dprint(f"[dim]清理 {len(stale_ids)} 条过期 detail 缓存[/dim]")

        # 对每个 job：从缓存取 endpoints；缓存未命中则拉取 detail 并缓存 endpoints
        # job 对象本身（status/duration 等）始终保持列表接口返回的新鲜值，不被替换
        fetched_count = 0
        hit_count = 0
        endpoints_map = {}   # {job_id: endpoints_dict}，本轮使用
        for j in jobs:
            job_id = j.get("metadata", {}).get("id", "")
            if not job_id:
                continue
            if job_id in cache:
                endpoints_map[job_id] = cache[job_id]
                hit_count += 1
            else:
                fetched = api.get_job(job_id)
                if fetched:
                    ep = fetched.get("endpoints", {})
                    endpoints_map[job_id] = ep
                    # 只有当 endpoints 中确实包含 SSH 端口信息时才写入缓存。
                    # 若 task_urls 为空，说明端口尚未分配，下次仍需重新拉取，
                    # 不能将"没有端口"这一临时状态当作确定结果缓存下来。
                    has_port = bool(ep.get("ssh", {}).get("task_urls"))
                    if has_port:
                        cache[job_id] = ep
                    fetched_count += 1
                # 拉取失败则 endpoints_map 中无此 key，后续会降级为空 SSH

        # 将更新后的缓存写回（只存 endpoints）
        save_detail_cache(cache)
        dprint(f"[dim]detail 缓存：命中 {hit_count} 条，新拉取 {fetched_count} 条[/dim]")

    if getattr(args, "json", False):
        if getattr(args, "detail", False):
            out = []
            for j in jobs:
                job_id = j.get("metadata", {}).get("id", "")
                ep = endpoints_map.get(job_id, {}) if job_id else {}
                ssh = enrich_ssh_entries(ep.get("ssh", {}).get("task_urls", []))
                out.append(job_to_dict(j, ssh_override=ssh))
            _json_out(out)
        else:
            _json_out([job_to_dict(j) for j in jobs])
        return

    if not jobs:
        cprint("[yellow]没有找到符合条件的训练作业[/yellow]"); return

    total = data.get("total", 0)
    t = Table(title=f"训练作业（总计 {total} 个，过滤后显示 {len(jobs)} 个）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#",     width=3)
    t.add_column("名称",  style="green", no_wrap=True, max_width=20)
    t.add_column("ID",    style="dim",   no_wrap=True, width=40)
    t.add_column("状态",  no_wrap=True,  width=13)
    t.add_column("时长",  width=10)
    t.add_column("卡数",  width=4)
    if getattr(args, "detail", False):
        t.add_column("SSH端口", width=12)
    t.add_column("创建时间", width=17)
    t.add_column("创建者",   width=10)

    for i, j in enumerate(jobs, 1):
        meta  = j.get("metadata", {})
        st    = j.get("status",   {})
        spec  = j.get("spec",     {})
        phase = st.get("phase", "?")
        color = STATUS_COLOR.get(phase, "white")
        gpu_num = spec.get("resource", {}).get("pool_info", {}).get("accelerator_num", "?")
        row = [
            str(i), meta.get("name", ""), meta.get("id", ""),
            f"[{color}]{phase}[/{color}]",
            ms_to_hms(st.get("duration")), f"{gpu_num}卡",
        ]
        if getattr(args, "detail", False):
            job_id = meta.get("id", "")
            ep = endpoints_map.get(job_id, {}) if job_id else {}
            ssh_entries = enrich_ssh_entries(ep.get("ssh", {}).get("task_urls", []))
            row.append(ssh_ports_summary(ssh_entries))
        row.extend([
            ts_to_str(meta.get("create_time")), meta.get("user_name", "")
        ])
        t.add_row(*row)
    console.print(t)
    if getattr(args, "detail", False):
        do_refresh = getattr(args, "refresh", False)
        if do_refresh:
            cprint("[dim]已清空缓存并重新拉取所有 detail（--refresh）[/dim]")
        else:
            cprint("[dim]已从 detail 缓存读取（未命中的条目已自动拉取并缓存）[/dim]")
    else:
        cprint("[dim]用 detail <JOB_ID> 查看 SSH 信息[/dim]")
def cmd_detail(args):
    # 无参数：等同于 jobs --detail
    if not getattr(args, "job_id", None) and not getattr(args, "src_name", None):
        import types
        list_args = types.SimpleNamespace(
            action=None, limit=50,
            recent=None, running=False, failed=False,
            terminated=False, pending=False, status=None,
            gpu_count=None, name=None,
            detail=True, refresh=getattr(args, "refresh", False),
            json=getattr(args, "json", False),
        )
        cmd_list_jobs(list_args)
        return

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


def _pick_log_task(tasks: list, preferred: str = None) -> str:
    if preferred:
        return preferred
    if not tasks:
        return "worker-0"
    if len(tasks) == 1:
        return tasks[0].get("task") or "worker-0"
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


def _usage_panel_text(result: dict) -> str:
    m = result["metrics"]
    lines = []
    rows = [
        ("CPU", "cpu_util", "cpu_used_core"),
        ("内存", "memory_util", "memory_used_megabytes"),
        ("GPU", "gpu_util", "gpu_mem_used_megabytes"),
        ("显存", "gpu_mem_util", "gpu_mem_used_megabytes"),
    ]
    start_ts = None
    end_ts = None
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
        avg_used = _fmt_usage_value(used_key, used.get("avg"))
        trend = _sparkline(util.get("values", []), width=36)
        color = 'green' if (latest_util or 0) < 0.5 else 'yellow' if (latest_util or 0) < 0.8 else 'red'
        lines.append(f"[bold]{title:<4}[/bold] [{color}]{trend}[/{color}] {percent}")
        lines.append(f"      latest={latest_used}   avg={avg_used}")
    if start_ts and end_ts:
        from datetime import datetime as _dt
        st = _dt.fromtimestamp(start_ts).strftime('%H:%M')
        ed = _dt.fromtimestamp(end_ts).strftime('%H:%M')
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
    for key, metric_name in metrics.items():
        query = _build_usage_query(metric_name, job_id)
        data = api.query_usage_range(query=query, start=start_ts, end=end_ts, step=step)
        series = (((data or {}).get("data") or {}).get("result") or [])
        values = (series[0].get("values") if series else []) or []
        result["metrics"][key] = _usage_series_stats(values)
    return result


def cmd_usage(args):
    sess = _sess_or_exit()
    api  = API(sess)

    if args.job_id:
        result = _fetch_usage_result(api, args.job_id, args.minutes, args.step)
        if getattr(args, "json", False):
            _json_out(result)
            return
        console.print(Panel(
            _usage_panel_text(result),
            title=f"作业监控  {args.job_id}",
            border_style="cyan",
        ))
        cprint(f"[dim]时间范围: 最近 {args.minutes} 分钟，step={args.step}s[/dim]")
        return

    jobs = api.list_jobs(limit=args.limit).get("items", [])
    running_jobs = [j for j in jobs if j.get("status", {}).get("phase") == "Running"]
    rows = []
    for job in running_jobs:
        meta = job.get("metadata", {})
        job_id = meta.get("id", "")
        name = meta.get("name", "")
        u = _fetch_usage_result(api, job_id, args.minutes, args.step)
        rows.append({
            "job_id": job_id,
            "name": name,
            "cpu": u["metrics"].get("cpu_util", {}).get("latest"),
            "mem": u["metrics"].get("memory_used_megabytes", {}).get("latest"),
            "gpu": u["metrics"].get("gpu_util", {}).get("latest"),
            "gpu_mem": u["metrics"].get("gpu_mem_used_megabytes", {}).get("latest"),
        })

    if getattr(args, "json", False):
        _json_out({
            "minutes": args.minutes,
            "step": args.step,
            "jobs": rows,
        })
        return

    t = Table(title="Running 作业最近 usage", header_style="bold cyan")
    t.add_column("名称", style="green")
    t.add_column("JOB_ID", style="dim")
    t.add_column("CPU")
    t.add_column("内存")
    t.add_column("GPU")
    t.add_column("显存")
    for row in rows:
        t.add_row(
            row["name"],
            row["job_id"],
            _fmt_usage_value("cpu_util", row["cpu"]),
            _fmt_usage_value("memory_used_megabytes", row["mem"]),
            _fmt_usage_value("gpu_util", row["gpu"]),
            _fmt_usage_value("gpu_mem_used_megabytes", row["gpu_mem"]),
        )
    console.print(t)
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
    task_name = _pick_log_task(tasks, preferred=args.task)

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


def cmd_exec(args):
    """通过 CloudShell WebSocket 执行任意远程命令，重定向 stdout/stderr 到本地。"""
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
        # macli exec JOB_ID -- cmd arg1 arg2  （argparse 会把 -- 也收进来）
        parts = args.inline_cmd
        if parts and parts[0] == "--":
            parts = parts[1:]
        script = " ".join(parts)
    else:
        cprint("[red]请指定命令：使用 -- <cmd>、--script <file> 或 --stdin[/red]")
        sys.exit(1)

    # ── 检查 exec 权限 ─────────────────────────────────────────
    status = api.get_exec_status(args.job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业当前不允许执行命令（CloudShell 未就绪）[/red]")
            sys.exit(1)

    tasks     = api.get_job_tasks(args.job_id)
    task_name = _pick_log_task(tasks, preferred=getattr(args, "task", None))

    dprint(f"[cyan]正在连接（task={task_name}）...[/cyan]")
    sock = _open_exec_ws(sess, args.job_id, task_name, command="/bin/bash")
    dprint("[green]✓ 已连接[/green]")

    # ── 构造远端指令序列 ───────────────────────────────────────
    # 用 base64 编码脚本，避免特殊字符/多行/heredoc 解析问题
    import base64 as _b64
    script_b64 = _b64.b64encode(script.encode()).decode()

    START_MARKER = "MACLI_EXEC_START_7f3a9"
    EXIT_MARKER  = "MACLI_EXEC_EXIT_7f3a9"
    TMP_B64      = "/tmp/.macli_exec_b64_$$"

    # 每次发一行，避免触发 PTY 行缓冲限制（≈4096 B）
    CHUNK = 512
    chunks = [script_b64[i:i+CHUNK] for i in range(0, len(script_b64), CHUNK)]

    setup_lines = [
        f"stty -echo; PS1=''; PS2=''\r",
        f"TMP={TMP_B64}; rm -f \"$TMP\"\r",
    ]
    for ch in chunks:
        setup_lines.append(f"printf '%s' '{ch}' >> \"$TMP\"\r")

    run_parts = ["base64 -d \"$TMP\" | bash"]
    if getattr(args, "cwd", None):
        cwd_esc = args.cwd.replace("'", "'\\''")
        run_parts = [f"cd '{cwd_esc}' &&"] + run_parts

    exec_line = (
        f"echo {START_MARKER}; "
        + " ".join(run_parts)
        + f"; echo {EXIT_MARKER}:$?; rm -f \"$TMP\"; exit\r"
    )
    setup_lines.append(exec_line)

    # ── 收集输出 ───────────────────────────────────────────────
    buf       = bytearray()
    exit_code = [None]
    done      = threading.Event()

    def reader():
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
                        text = buf.decode("utf-8", errors="replace")
                        if EXIT_MARKER in text:
                            m = re.search(rf"{EXIT_MARKER}:(\d+)", text)
                            if m:
                                exit_code[0] = int(m.group(1))
                            done.set()
        except Exception as e:
            _raw_debug(f"exec reader error: {type(e).__name__}: {e}")
        done.set()

    def heartbeat():
        while not done.is_set():
            done.wait(timeout=5)
            if done.is_set():
                break
            try:
                _ws_send_frame(sock, b"\x00", opcode=2)
            except Exception:
                break

    threading.Thread(target=reader,    daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()

    # ── 发送指令 ───────────────────────────────────────────────
    time.sleep(0.4)   # 等 bash 启动就绪
    for line in setup_lines:
        _ws_send_frame(sock, b"\x00" + line.encode(), opcode=2)
        time.sleep(0.02)

    timeout = getattr(args, "timeout", 300)
    done.wait(timeout=timeout)
    if not done.is_set():
        cprint(f"[red]执行超时（{timeout}s）[/red]", file=sys.stderr)

    try:
        sock.close()
    except Exception:
        pass

    # ── 后处理：截取 START~EXIT 之间的内容 ─────────────────────
    raw_text = buf.decode("utf-8", errors="replace")

    # 提取 START_MARKER 之后的部分
    if START_MARKER in raw_text:
        raw_text = raw_text.split(START_MARKER, 1)[1].lstrip("\r\n")
    # 截掉 EXIT_MARKER 行及之后
    if EXIT_MARKER in raw_text:
        raw_text = raw_text[:raw_text.index(EXIT_MARKER)]

    # 去除 ANSI 转义码和多余 \r
    clean = re.sub(r"\x1b\[[0-9;]*[A-Za-z]|\r", "", raw_text)

    sys.stdout.write(clean)
    if clean and not clean.endswith("\n"):
        sys.stdout.write("\n")

    sys.exit(exit_code[0] if exit_code[0] is not None else 1)


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


def cmd_stop(args):
    sess = _sess_or_exit()
    api  = API(sess)
    job  = api.get_job(args.job_id)
    if not job: sys.exit(1)

    name  = job.get("metadata", {}).get("name", "")
    phase = job.get("status", {}).get("phase", "")

    console.print(Panel(
        f"[bold]作业:[/bold] {name}\n"
        f"[bold]状态:[/bold] {phase}",
        title="终止训练作业", border_style="red"))

    if not args.yes:
        ok = Confirm.ask("[red]确认终止？[/red]")
        if not ok: cprint("[yellow]已取消[/yellow]"); return

    if api.stop_job(args.job_id):
        cprint("[green]✓ 已发送终止指令[/green]")
    else:
        cprint("[red]终止失败[/red]")


def cmd_delete(args):
    sess = _sess_or_exit()
    api  = API(sess)

    # 收集所有作业信息
    jobs_info = []
    for job_id in args.job_ids:
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

def main():
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

macli jobs [filters...] [--limit LIMIT] [--detail] [--json]  # 列出作业列表，支持多种过滤条件
macli jobs [filters...] [--detail] [--refresh] [--json]      # --refresh 清空 detail 缓存并重新拉取
macli jobs count [filters...] [--json] # 仅返回满足过滤条件的作业数量

# jobs filters:
#   [--name <NAME>]
#   [--recent <DURATION>]  # e.g. 1h, 30m, 2d, 1y, etc.  m means months, y means years
#   [--running] [--failed] [--terminated] [--pending] | [--status STATUS [STATUS ...]]
#   [--gpu-count N [N ...]]

macli detail <JOB_ID> [--json]
macli detail --name <JOB_NAME> [--json]

macli events <JOB_ID> [--limit LIMIT] [--offset OFFSET] [--json]
macli log <JOB_ID> --output <OUTPUT_PATH> [--task TASK]
macli usage [<JOB_ID>] [--minutes N] [--step N] [--json]
macli shell <JOB_ID> [--task TASK]
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

    sub.add_parser("logout", help="清除已保存的登录凭据（config/session.json）")

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
    q.add_argument("--detail",     action="store_true",
                   help="对过滤后的每个作业额外拉取 detail；表格会显示 SSH 端口，JSON 会并入详情字段；优先使用本地缓存")
    q.add_argument("--refresh",    action="store_true",
                   help="与 --detail 配合使用：清空所有缓存，强制重新拉取所有符合条件的 detail 并重建缓存")
    q.add_argument("--json",       action="store_true", help="JSON 输出")

    q = sub.add_parser("detail", help="作业详情 + SSH 信息；无参数时等同于 jobs --detail")
    grp = q.add_mutually_exclusive_group(required=False)
    grp.add_argument("job_id",     metavar="JOB_ID",  nargs="?", default=None,
                     help="作业 ID；省略则列出所有作业（含 detail）")
    grp.add_argument("--name",     dest="src_name",   default=None,
                     help="按作业名称查找（取最新一个）")
    q.add_argument("--refresh",    action="store_true",
                   help="清空 detail 缓存并强制重新拉取（仅无参数模式有效）")
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
    q.add_argument("--json", action="store_true", help="JSON 输出")

    q = sub.add_parser("shell", help="打开作业 CloudShell 交互终端")
    q.add_argument("job_id", metavar="JOB_ID", help="作业 ID")
    q.add_argument("--task", default=None, help="任务名，例如 worker-0；默认自动选择")
    q.add_argument("--heartbeat", type=float, default=2.0, help="空闲时发送心跳包的间隔秒数，默认 2")

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
""")
    q.add_argument("job_id", metavar="JOB_ID", help="作业 ID")
    q.add_argument("--task",    default=None, help="任务名，例如 worker-0；默认自动选择")
    q.add_argument("--cwd",     default=None, metavar="DIR", help="执行命令前先切换到指定目录")
    q.add_argument("--timeout", type=int, default=300, help="等待命令结束的超时秒数，默认 300")
    src = q.add_mutually_exclusive_group()
    src.add_argument("--script", dest="script_file", default=None, metavar="FILE",
                     help="从本地文件读取要执行的脚本")
    src.add_argument("--stdin",  dest="use_stdin",   action="store_true",
                     help="从 stdin 读取要执行的脚本")
    q.add_argument("inline_cmd", metavar="CMD", nargs=argparse.REMAINDER,
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

    q = sub.add_parser("stop", help="终止训练作业")
    q.add_argument("job_id", metavar="JOB_ID")
    q.add_argument("-y", "--yes", action="store_true")

    q = sub.add_parser("delete", help="删除训练作业（支持空格隔开批量删除）")
    q.add_argument("job_ids", metavar="JOB_ID", nargs="+")
    q.add_argument("-f", "--force", action="store_true", help="强制删除（包括运行中的作业）")
    q.add_argument("-y", "--yes",   action="store_true")

    args = p.parse_args()

    # --command 中支持字面 \n 转换为真正换行
    if hasattr(args, "command") and args.command:
        args.command = args.command.replace("\\n", "\n")

    global _VERBOSE
    _VERBOSE = getattr(args, "debug", False)

    if args.cmd == "region":
        {"list": cmd_region_list, "select": cmd_region_select}[args.rg_cmd](args)
    elif args.cmd == "workspace":
        {"list": cmd_workspace_list, "select": cmd_workspace_select}[args.ws_cmd](args)
    else:
        {"login":     cmd_login,
         "logout":    cmd_logout,
         "whoami":    cmd_whoami,
         "jobs":      cmd_list_jobs,
         "detail":    cmd_detail,
         "events":    cmd_events,
         "log":       cmd_log,
         "usage":     cmd_usage,
         "shell":     cmd_shell,
         "exec":      cmd_exec,
         "copy":      cmd_copy,
         "stop":      cmd_stop,
         "delete":    cmd_delete}[args.cmd](args)

if __name__ == "__main__":
    try:
        main()
    except SessionExpiredError as e:
        console.print(f"\n[bold red]✗ 登录已过期[/bold red]  {e}")
        console.print("[yellow]请重新执行：[/yellow] [bold]macli login[/bold]")
        sys.exit(2)
