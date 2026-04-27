"""配置 I/O：session.json 读写、各子域配置、凭据管理"""
import os, json
from pathlib import Path

# ── keyring 探测 ──────────────────────────────────────────────
try:
    import keyring as _keyring
    _keyring.get_password("macli", "_probe")
    _KEYRING_OK = True
except Exception:
    _keyring = None
    _KEYRING_OK = False

_KR_SERVICE = "macli"
_KR_KEY     = "credentials"


# ── 核心 session 读写 ────────────────────────────────────────

def _config_path() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "macli" / "session.json"


def load_session() -> dict:
    try:
        p = _config_path()
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_session(data: dict):
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── 通用子域配置 helper ──────────────────────────────────────

def _load_cfg_section(key: str) -> dict:
    return load_session().get(key, {})


def _save_cfg_section(key: str, cfg):
    data = load_session()
    data[key] = cfg
    save_session(data)


# ── 具名包装 ─────────────────────────────────────────────────

_AUTOLOGIN_KEY = "auto_login"
_WATCH_KEY     = "watch"
_SERVER_KEY    = "server"


def load_auto_login_cfg() -> dict:
    return _load_cfg_section(_AUTOLOGIN_KEY)

def save_auto_login_cfg(cfg: dict):
    _save_cfg_section(_AUTOLOGIN_KEY, cfg)

def load_watch_cfg() -> dict:
    return _load_cfg_section(_WATCH_KEY)

def save_watch_cfg(cfg: dict):
    _save_cfg_section(_WATCH_KEY, cfg)

def load_server_cfg() -> dict:
    return _load_cfg_section(_SERVER_KEY)

def save_server_cfg(cfg: dict):
    _save_cfg_section(_SERVER_KEY, cfg)


# ── SSH 密钥管理 ─────────────────────────────────────────────

def load_identityfiles() -> tuple:
    data = load_session()
    return data.get("identityfiles", {}), data.get("default_identityfile", None)


def save_identityfiles(files: dict, default: str = None):
    data = load_session()
    data["identityfiles"] = files
    data["default_identityfile"] = default
    save_session(data)


def get_exec_backend() -> str:
    return load_session().get("exec_backend", "cloudshell")


def set_exec_backend(backend: str):
    data = load_session()
    data["exec_backend"] = backend
    save_session(data)


# ── 凭据安全存储 ─────────────────────────────────────────────

_CREDS_FILE = _config_path().parent / "credentials.json"


def _load_saved_creds() -> dict:
    if _KEYRING_OK:
        try:
            raw = _keyring.get_password(_KR_SERVICE, _KR_KEY)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    try:
        if _CREDS_FILE.exists():
            return json.loads(_CREDS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_creds(domain: str, username: str, password: str) -> bool:
    from macli.log import dprint
    payload = json.dumps({"domain": domain, "username": username, "password": password},
                         ensure_ascii=False)
    if _KEYRING_OK:
        try:
            _keyring.set_password(_KR_SERVICE, _KR_KEY, payload)
            return True
        except Exception:
            pass
    try:
        _CREDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CREDS_FILE.write_text(payload, encoding="utf-8")
        _CREDS_FILE.chmod(0o600)
        dprint("[dim]密码已存入文件（keyring 不可用）[/dim]")
        return True
    except Exception:
        return False


def _clear_saved_creds() -> bool:
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
