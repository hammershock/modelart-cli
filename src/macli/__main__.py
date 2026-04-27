#!/usr/bin/env python3
"""
华为云 ModelArts 远程管理 CLI
author: @hammershock
version: 0.2.0
"""
import os, sys, argparse
from pathlib import Path

# 确保依赖可用（缺失时自动安装并重启）
from macli.net import _ensure_pkg
_ensure_pkg("requests", "rich")

from macli.constants import SessionExpiredError, console
from macli.config import load_auto_login_cfg, load_watch_cfg
from macli.log import _init_logger, _flog, set_verbose, dprint
from macli.auth import _do_auto_login, _autologin_record_outcome
from macli.platform_daemon import _run_check_once

# 命令处理函数（延迟导入以加速启动）
from macli.commands.login import cmd_login, cmd_logout
from macli.commands.region import (cmd_region_list, cmd_region_select,
                                   cmd_workspace_list, cmd_workspace_select,
                                   cmd_whoami)
from macli.commands.jobs import cmd_list_jobs, cmd_query, cmd_ports
from macli.commands.detail import cmd_detail, cmd_events
from macli.commands.log_cmd import cmd_log
from macli.commands.usage import cmd_usage
from macli.commands.shell import cmd_shell
from macli.commands.ssh import cmd_ssh
from macli.commands.exec_ import cmd_exec
from macli.commands.ops import cmd_copy, cmd_stop, cmd_delete
from macli.commands.identityfile import cmd_identityfile
from macli.commands.autologin import cmd_autologin
from macli.commands.watch import cmd_watch
from macli.commands.server import cmd_server
from macli.commands.alert_email import cmd_alert_email


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
macli shell <JOB_ID> [--task TASK]
macli ssh <JOB_ID> [--task TASK] [--identityfile PATH/NAME]
macli identityfile add <PATH> [--name/-n <NAME>]
macli identityfile remove <NAME>
macli identityfile list
macli identityfile default [--set <PATH/NAME>]
macli alert-email add <EMAIL>
macli alert-email remove <EMAIL>
macli alert-email status
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
    q.add_argument("--interval", type=float, default=None, metavar="H",
                   help="检查间隔（小时），默认 1")
    q.add_argument("--script",   default=None, metavar="PATH",
                   help="check_jobs.py 的路径（enable/run 时使用）")
    q.add_argument("--threshold-hours", dest="threshold_hours", type=int, default=72,
                   metavar="N", help="Terminated 作业保留时长（小时），默认 72")

    ae = sub.add_parser("alert-email", help="管理磁盘风险邮件提醒").add_subparsers(
        dest="alert_email_action", required=True
    )
    ae.add_parser("status", help="显示邮件提醒配置")
    ae.add_parser("list", help="列出提醒邮箱")
    q = ae.add_parser("add", help="添加提醒邮箱")
    q.add_argument("email", metavar="EMAIL")
    q = ae.add_parser("remove", help="移除提醒邮箱")
    q.add_argument("email", metavar="EMAIL")
    ae.add_parser("enable", help="启用邮件提醒")
    ae.add_parser("disable", help="停用邮件提醒")
    q = ae.add_parser("smtp", help="配置 SMTP")
    q.add_argument("--host", default=None, help="SMTP host")
    q.add_argument("--port", type=int, default=None, help="SMTP port")
    q.add_argument("--username", default=None, help="SMTP username")
    q.add_argument("--password", default=None, help="SMTP password/auth code")
    q.add_argument("--security", choices=["ssl", "starttls", "none"], default=None,
                   help="SMTP 加密方式")
    q.add_argument("--from-email", dest="from_email", default=None, help="发件邮箱")
    q.add_argument("--throttle-hours", type=float, default=None,
                   help="同类磁盘告警最小发送间隔，默认 12")
    q.add_argument("--enable", action="store_true", help="配置后立即启用")
    q = ae.add_parser("import-yaml", help="从 mail_accounts.yaml 导入 SMTP")
    q.add_argument("path", metavar="PATH")
    q.add_argument("--provider", default="163", help="按 provider/email 匹配，默认 163")
    q.add_argument("--email", default=None, help="指定要导入的邮箱地址")
    q.add_argument("--recipient", action="append", default=None,
                   help="添加提醒收件人；不指定则使用导入的邮箱")
    q.add_argument("--throttle-hours", type=float, default=12,
                   help="同类磁盘告警最小发送间隔，默认 12")
    q.add_argument("--enable", action="store_true", help="导入后启用")
    q = ae.add_parser("test", help="发送测试邮件")
    q.add_argument("--to", action="append", default=None, help="测试收件人；默认使用提醒邮箱")
    q = ae.add_parser("check-disk", help="检查磁盘快照并按需发送告警")
    q.add_argument("--snapshot", default=str(Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "macli" / "disk_state.json"))
    q.add_argument("--force", action="store_true", help="忽略 12h 限流立即发送")
    q.add_argument("--json", action="store_true", help="只输出风险作业 JSON，不发送")

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

    args, _remaining = p.parse_known_args()
    if _remaining:
        if getattr(args, "cmd", None) == "exec":
            extra = _remaining[1:] if _remaining[0] == "--" else _remaining
            args.inline_cmd = list(getattr(args, "inline_cmd", None) or []) + extra
        else:
            p.error(f"unrecognized arguments: {' '.join(_remaining)}")

    if hasattr(args, "command") and args.command:
        args.command = args.command.replace("\\n", "\n")

    set_verbose(getattr(args, "debug", False))

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
         "alert-email":  cmd_alert_email,
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
            cfg = load_auto_login_cfg()
            if cfg.get("enabled") and not os.environ.get("MACLI_NO_AUTOLOGIN"):
                dprint(f"[dim]SessionExpiredError: {e}，触发自动登录[/dim]")
                ok = _do_auto_login(cfg)
                if ok:
                    _autologin_record_outcome(True)
                    wcfg = load_watch_cfg()
                    if wcfg.get("enabled"):
                        dprint("[dim]自动登录后触发 watch run[/dim]")
                        sp = wcfg.get("script_path", "")
                        if sp and Path(sp).exists():
                            _run_check_once(Path(sp), wcfg.get("threshold_hours", 72))
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
