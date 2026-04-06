---
name: modelart-cli
description: 通过 `macli` 命令行工具管理华为云 ModelArts
---

### 命令选型：
- login 登录
- autologin 管理会话过期时的自动重新登录（含 ntfy 推送）
- whoami 显示当前登录用户信息
- region 切换和显示当前region
- workspace 切换和显示当前workspace
- jobs 列出作业列表，支持多种过滤条件（含 SSH 端口）
- query 按条件筛选作业，输出 ID，可管道给 stop/delete/exec 等批量操作
- ports 快速列出所有 Running 作业的 SSH 端口
- detail 显示作业详情, 包括ssh连接信息等
- events 显示作业事件列表
- log 下载完整日志文件（OBS 原始日志）
- usage 查看作业 CPU/内存/GPU/显存实时监控数据
- shell 打开作业 CloudShell 交互终端（SSH-like）
- ssh 通过原生 SSH 直接连接作业容器
- identityfile 管理 SSH 密钥（add/remove/list/default）
- exec 在作业容器内执行命令（支持 cloudshell / ssh 两种后端）
- copy 以一个作业为模板创建新作业, 可以选择覆盖GPU数量、命令、名称和描述等
- stop 终止作业，支持多 ID 和管道输入
- delete 彻底删除作业，支持多 ID 和管道输入

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

macli autologin [status|enable|disable] [--retries N] [--timeout SECS] [--reset-topic]
# status（默认）查看配置；enable/disable 开关自动重登；--reset-topic 重新生成 ntfy 推送 topic

macli jobs [filters...] [--limit LIMIT] [--refresh] [--json]  # 列出作业列表，支持多种过滤条件（含 SSH 端口）
macli jobs count [filters...] [--json]  # 仅返回满足过滤条件的作业数量

# jobs filters:
#   [--name <NAME>]
#   [--recent <DURATION>]  # e.g. 1h, 30m, 2d, 1y, etc.  m means months, y means years
#   [--running] [--failed] [--terminated] [--pending] | [--status STATUS [STATUS ...]]
#   [--gpu-count N [N ...]]
#   [--refresh]  # 清空端口缓存，强制重新拉取

macli ports [--refresh] [--json]  # 快速列出所有 Running 作业的 SSH 端口

# ── query：批量选取作业，管道给其他命令 ────────────────────────────────────
macli query [filters...] [--json]
# filters（同 jobs）:
#   [--name NAME] [--recent DURATION]
#   [--running] [--failed] [--terminated] [--pending] | [--status STATUS...]
#   [--gpu-count N...] [--limit N]
# 终端运行：显示预览表格 + 使用提示
# 管道运行：每行输出一个 JOB_ID（干净，适合传给下游命令）

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

macli ssh <JOB_ID> [--task worker-0] [-i PATH/NAME] [-o SSH_OPTION]
# 通过原生 SSH 连接作业容器（需提前配置 SSH 密钥）
# -i / --identityfile  私钥路径或已保存的密钥名称；不指定则使用默认密钥
# -o / --opt           追加额外 SSH 选项，可多次使用

macli identityfile add <PATH> [--name ALIAS]   # 添加 SSH 密钥（可设别名）
macli identityfile remove <NAME>               # 移除已保存的 SSH 密钥
macli identityfile list                        # 列出所有已保存的 SSH 密钥
macli identityfile default [--set PATH/NAME]   # 查看或设置默认密钥

macli exec <JOB_ID> [--task worker-0] [--cwd DIR] [--timeout 300] [--backend cloudshell|ssh]
           [-i PATH/NAME] [-o SSH_OPTION]
           [-- CMD ARGS...]           # 执行简单命令（-- 后接）
           [--script FILE]            # 从本地脚本文件执行
           [--stdin]                  # 从 stdin 读取脚本
macli exec --backend ssh              # 仅切换默认后端，不执行命令
# 后端选择自动记忆，下次无需重复指定
# 批量模式：macli query [filters] | macli exec -- <cmd>
#   无 JOB_ID 且 stdin 非脚本时，从 stdin 读取 ID，顺序对每个作业执行同一命令

macli copy <JOB_ID> [options...] [-y | --yes] [--json]
macli copy --src-name <SRC_NAME> [options...] [-y | --yes] [--json]

# copy options:
#   [--gpu-count N]  # 覆盖原有的GPU数量
#   [--name NEW_NAME]  # 新拷贝的作业名称
#   [--desc NEW_DESC]  # 新拷贝的作业描述
#   [--command COMMAND | --command-file COMMAND_FILE]  # 覆盖新拷贝的启动命令

macli stop [JOB_ID ...] [-y | --yes]
# 支持多个 ID（空格分隔）或从 stdin 读取（macli query ... | macli stop）

macli delete [JOB_ID ...] [-y | --yes] [-f | --force]
# -f/--force 强制删除运行中的作业；支持多 ID 或管道输入
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
macli ports  # 列出所有 Running 作业的 SSH 端口
macli ports --refresh  # 清空缓存后重新拉取
macli shell e40422c7-f151-4cba-982f-957c368071e3  # 打开 CloudShell 交互终端
macli --debug shell e40422c7-f151-4cba-982f-957c368071e3  # 以调试模式打开（可见心跳动画和帧信息）
macli ssh e40422c7-f151-4cba-982f-957c368071e3  # 通过原生 SSH 连接作业（使用默认密钥）
macli ssh e40422c7-f151-4cba-982f-957c368071e3 -i ~/.ssh/id_rsa  # 指定 SSH 私钥
macli identityfile add ~/.ssh/id_rsa --name mykey  # 添加 SSH 密钥并起别名
macli identityfile default --set mykey  # 设置默认 SSH 密钥
macli exec e40422c7-f151-4cba-982f-957c368071e3 -- nvidia-smi  # 在容器内执行命令
macli exec e40422c7-f151-4cba-982f-957c368071e3 --script run.sh  # 执行本地脚本
macli exec e40422c7-f151-4cba-982f-957c368071e3 --backend ssh -- nvidia-smi  # 通过 SSH 后端执行
macli autologin enable  # 开启会话过期自动重登
macli copy --src-name template --gpu-count 1 --name "my-exp" --desc "1卡实验" --command "mkdir /cache\\nsleep 2000000000s;" --yes  # 以名为template的作业为模板创建一个新作业，覆盖GPU数量和启动命令，并跳过确认提示
macli copy e40422c7-f151-4cba-982f-957c368071e3 --gpu-count 2 --name "my-exp-2" --desc "2卡实验" --command-file start.sh --yes  # 以指定ID的作业为模板创建一个新作业，覆盖GPU数量和启动命令，并跳过确认提示
macli stop e40422c7-f151-4cba-982f-957c368071e3 --yes  # 停止一个正在运行的作业，跳过确认提示
macli delete e40422c7-f151-4cba-982f-957c368071e3 --force --yes  # 删除一个作业，跳过确认提示, 如果作业正在运行则强制删除

# ── query 批量操作示例 ────────────────────────────────────────────────────────
macli query --running                              # 终端：预览所有 Running 作业表格
macli query --failed --recent 1d                   # 终端：预览最近 1 天内失败的作业
macli query --running --gpu-count 8                # 终端：预览 8 卡 Running 作业

macli query --running | macli stop --yes           # 终止所有 Running 作业（需确认）
macli query --failed --recent 1d | macli delete --yes   # 删除最近 1 天内的失败作业
macli query --running --name "exp-v2" | macli stop --yes  # 终止名为 exp-v2 的所有 Running 作业

macli query --running | macli exec -- nvidia-smi   # 对所有 Running 作业执行 nvidia-smi
macli query --running | macli exec -- df -h /cache # 检查所有 Running 作业的 /cache 空间
macli query --running | macli exec --script check.sh   # 对所有 Running 作业执行本地脚本
macli query --running --backend ssh | macli exec -- nvidia-smi  # SSH 后端批量执行
```

### query 详细使用说明：

`macli query` 是批量操作的核心工具，设计为标准 Unix 管道组件：

```bash
# 选取条件与 jobs 完全一致，支持组合过滤：
macli query [--name NAME] [--recent DURATION]
            [--running | --failed | --terminated | --pending]
            [--status STATUS...] [--gpu-count N...] [--limit N]

# 终端运行（stdout 是 TTY）：显示预览表格，不输出 ID 到标准输出
macli query --running

# 管道运行（stdout 非 TTY）：每行输出一个 JOB_ID，无任何额外信息
macli query --running | cat
macli query --running | wc -l  # 统计 Running 作业数量

# JSON 输出（含 id/name/status）：
macli query --running --json

# 管道给 stop（显示预览表 → 一次性确认 → 批量终止）：
macli query --running | macli stop --yes

# 管道给 delete（--force 允许删除 Running 作业）：
macli query --failed | macli delete --yes
macli query --running | macli delete --force --yes

# 管道给 exec（对每个作业顺序执行，输出带作业名称分隔标题）：
macli query --running | macli exec -- nvidia-smi
# 输出示例：
# ══ job-name-1 (abc12345…) ══
# Mon Apr  7 ...  Driver Version: 525.89  ...
# ══ job-name-2 (def67890…) ══
# Mon Apr  7 ...  Driver Version: 525.89  ...
# 完成：全部 2 个作业执行成功

# exec 批量时可切换后端（SSH 更稳定）：
macli exec --backend ssh   # 先切换默认后端
macli query --running | macli exec -- nvidia-smi

# 与 xargs 组合（当需要并行执行时）：
macli query --running | xargs -P4 -I{} macli exec {} -- nvidia-smi
```
