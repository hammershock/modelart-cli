---
name: modelart-cli
description: 通过 `macli` 命令行工具管理华为云 ModelArts
---

### 命令选型：
- login 登录
- whoami 显示当前登录用户信息
- region 切换和显示当前region
- workspace 切换和显示当前workspace
- jobs 列出作业列表，支持多种过滤条件
- detail 显示作业详情, 包括ssh连接信息等
- events 显示作业事件列表
- log 下载完整日志文件（OBS 原始日志）
- usage 查看作业 CPU/内存/GPU/显存实时监控数据
- shell 打开作业 CloudShell 交互终端（SSH-like）
- copy 以一个作业为模板创建新作业, 可以选择覆盖GPU数量、命令、名称和描述等
- stop 停止一个正在运行的作业
- delete 彻底删除一个作业

### 全局选项：
```bash
macli --debug <command>  # 调试模式：输出详细内部信息（WebSocket 帧、心跳等）
```

### 常用命令:
```bash
macli login  # cli版登录，适用于大多数环境
macli login --cookie <COOKIE_STRING>  # 直接使用cookie字符串登录

macli whoami [--json]
macli logout  # 退出登录，清除嵌入在脚本尾部的cookie

macli region list [--json]  # 列出可用的region列表
macli region select  # 交互式选择当前region
macli region select --name <REGION_NAME>  # 切换区域，例如 REGION_NAME cn-north-9 为 华北-乌兰察布一

macli workspace list [--json]  # 列出可用的workspace列表
macli workspace select  # 交互式选择当前workspace
macli workspace select --name <WORKSPACE_NAME>
macli workspace select --id <WORKSPACE_ID>

macli jobs [filters...] [--limit LIMIT] [--json]  # 列出作业列表，支持多种过滤条件
macli jobs count [filters...] [--json] # 仅返回满足过滤条件的作业数量

# jobs filters:
#   [--name <NAME>]
#   [--recent <DURATION>]  # e.g. 1h, 30m, 2d, 1y, etc.  m means months, y means years
#   [--running] [--failed] [--terminated] [--pending] | [--status STATUS [STATUS ...]]
#   [--gpu-count N [N ...]]

macli detail <JOB_ID> [--json]
macli detail --name <JOB_NAME> [--json]

macli events <JOB_ID> [--limit N] [--offset N] [--json]  # 查看作业事件（K8s 事件、平台告警等）

macli log <JOB_ID> --output <OUTPUT_PATH> [--task worker-0] [--timeout 120] [--json]  # 下载完整日志文件

macli usage [JOB_ID] [--minutes N] [--step N] [--limit N] [--json]
# JOB_ID 可选：不填则列出所有 Running 作业的最近 usage 汇总
# --minutes   最近多少分钟（默认 15）
# --step      采样步长秒数（默认 60）
# --limit     无 JOB_ID 时最多检查多少个作业（默认 50）

macli shell <JOB_ID> [--task worker-0] [--heartbeat 2.0]
# 打开 CloudShell 交互终端，行为类似 ssh：
#   - 终端大小自动同步（SIGWINCH）
#   - 退出方式：在远端 shell 中输入 exit 即可
#   - --heartbeat   空闲时发送心跳包的间隔秒数（默认 2.0）
#   - --debug 模式下在终端右上角显示心跳闪烁动画（♥/♡）

macli copy <JOB_ID> [options...] [-y | --yes] [--json]
macli copy --src-name <SRC_NAME> [options...] [-y | --yes] [--json]

# copy options:
#   [--gpu-count N]  # 覆盖原有的GPU数量
#   [--name NEW_NAME]  # 新拷贝的作业名称
#   [--desc NEW_DESC]  # 新拷贝的作业描述
#   [--command COMMAND | --command-file COMMAND_FILE]  # 覆盖新拷贝的启动命令

macli stop <JOB_ID> [-y | --yes]

macli delete <JOB_ID> [-y | --yes] [-f | --force]  # -f/--force 会强制删除正在运行的作业, -y/--yes 会跳过删除确认提示
```

### 示例：
```bash
macli logout  # 退出登录,会清除嵌入在脚本尾部的cookie
macli login  # cli版登录，适用于大多数环境
macli region select --name cn-north-9  # 切换到华北-乌兰察布一
macli workspace select --name SAI2
macli jobs --recent 1d --running  # 列出最近一天内提交的正在运行的作业
macli detail e40422c7-f151-4cba-982f-957c368071e3  # 显示作业详情, 包括ssh连接信息等
macli events e40422c7-f151-4cba-982f-957c368071e3 --limit 20  # 查看最近 20 条作业事件
macli log e40422c7-f151-4cba-982f-957c368071e3 --output ./output.log  # 下载作业完整日志
macli usage  # 查看所有 Running 作业的最近 15 分钟 CPU/内存/GPU 用量
macli usage e40422c7-f151-4cba-982f-957c368071e3 --minutes 60  # 查看指定作业最近 1 小时用量
macli shell e40422c7-f151-4cba-982f-957c368071e3  # 打开 CloudShell 交互终端
macli --debug shell e40422c7-f151-4cba-982f-957c368071e3  # 以调试模式打开（可见心跳动画和帧信息）
macli copy --src-name template --gpu-count 1 --name "my-exp" --desc "1卡实验" --command "mkdir /cache\\nsleep 2000000000s;" --yes  # 以名为template的作业为模板创建一个新作业，覆盖GPU数量和启动命令，并跳过确认提示
macli copy e40422c7-f151-4cba-982f-957c368071e3 --gpu-count 2 --name "my-exp-2" --desc "2卡实验" --command-file start.sh --yes  # 以指定ID的作业为模板创建一个新作业，覆盖GPU数量和启动命令，并跳过确认提示
macli stop e40422c7-f151-4cba-982f-957c368071e3 --yes  # 停止一个正在运行的作业，跳过确认提示
macli delete e40422c7-f151-4cba-982f-957c368071e3 --force --yes  # 删除一个作业，跳过确认提示, 如果作业正在运行则强制删除
```
