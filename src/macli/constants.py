"""常量、Rich console 单例、异常类"""
import sys
import re
from datetime import timezone, timedelta

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm
from rich.progress import (
    Progress, TextColumn, BarColumn, DownloadColumn,
    TransferSpeedColumn, TimeRemainingColumn,
)

try:
    from rich.syntax import Syntax
except Exception:
    Syntax = None

_CST = timezone(timedelta(hours=8))

console = Console()

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

STATUS_COLOR = {
    "Running": "green", "Pending": "yellow", "Waiting": "yellow",
    "Failed": "red", "Completed": "blue", "Stopped": "dim",
}

_STATUS_ALIAS = {
    "running": "Running", "failed": "Failed",
    "terminated": "Stopped", "stopped": "Stopped",
    "pending": "Pending", "waiting": "Waiting",
    "completed": "Completed",
}

_ME_PROBE_REGIONS = [
    "cn-north-4", "cn-north-9", "cn-east-3", "cn-south-1",
    "cn-east-4", "cn-southwest-2", "ap-southeast-3",
]

_IS_LINUX = sys.platform.startswith("linux")


class SessionExpiredError(Exception):
    """登录凭据已过期，需要重新登录"""
    pass
