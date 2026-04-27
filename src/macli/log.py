"""日志/输出工具"""
import os, sys, re
from pathlib import Path
from datetime import datetime

from macli.constants import _CST, console

_VERBOSE = False
_LOG_PATH: "Path | None" = None
_RICH_TAG_RE = re.compile(r'\[[^\]\n]*?\]')


def set_verbose(flag: bool):
    global _VERBOSE
    _VERBOSE = flag


def is_verbose() -> bool:
    return _VERBOSE


def _strip_rich(s: str) -> str:
    """去除 Rich 样式标签，保留纯文本内容。"""
    return _RICH_TAG_RE.sub('', s)


def _flog(level: str, msg: str):
    """向日志文件追加一条记录。"""
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
    """初始化日志文件路径。"""
    global _LOG_PATH
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    _LOG_PATH = base / "macli" / "macli.log"
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        _LOG_PATH = None


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
    """始终记录到日志文件；仅 --debug 模式下输出到控制台。"""
    _flog("DEBUG", str(msg))
    if _VERBOSE:
        console.print(msg, style=style)


def _raw_debug(msg: str):
    """raw tty 模式下向 stderr 输出调试信息。"""
    if _VERBOSE:
        os.write(sys.stderr.fileno(), f"\r\033[K\033[2m[dbg] {msg}\033[m\r\n".encode())


def _status_debug(msg: str):
    """将调试状态锁定在终端第一行原地刷新。"""
    if _VERBOSE:
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 80
        short = msg[:cols - 2]
        os.write(sys.stderr.fileno(),
                 f"\033[s\033[1;1H\033[K\033[2m{short}\033[m\033[u".encode())
