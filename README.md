# macli

华为云 ModelArts 远程管理 CLI

## 环境定位

`macli` 管理的是华为云 **ModelArts 训练作业 / 训练容器**，不是 ECS（弹性云服务器）。SSH 连接进入的是 ModelArts 为 Running 训练作业暴露的容器端口，例如 `dev-modelarts-cnnorth9.huaweicloud.com:<port>`；这里的端口号对应作业容器的临时映射，不是 ECS 实例 ID，也不是一台可长期运维的虚拟机。

## 安装

```bash
git clone https://github.com/hammershock/modelart-cli.git
git pull
cd path/to/modelart-cli/
pip install -e .
```

## 快速开始

```bash
macli login 
macli region select --name cn-north-9
macli workspace select --name SAI2
macli identityfile add /path/to/your/sshidentityfile
macli exec --backend ssh
macli watch enable
macli watch run
macli server enable --region cn-north-9 --workspace SAI2
macli autologin enable

macli jobs --running            # 列出运行中的作业
macli usage                     # 查看 GPU/CPU 用量
macli exec <JOB_ID> -- nvidia-smi   # 在容器内执行命令
macli shell <JOB_ID>            # 打开交互终端
```

## 主要功能

| 命令 | 说明 |
|------|------|
| `login` / `logout` | 登录/退出 |
| `autologin` | 会话过期自动重登（webhook / ntfy 获取验证码） |
| `jobs` / `query` | 列出/筛选作业，支持管道批量操作 |
| `usage` | CPU / 内存 / GPU 实时监控 |
| `shell` / `ssh` | 交互终端（CloudShell 或原生 SSH） |
| `exec` | 在容器内执行命令或脚本 |
| `copy` | 以现有作业为模板创建新作业 |
| `log` | 下载完整日志文件 |
| `watch` | 定时保活 + 清理 Terminated 作业（cron / launchd） |
| `server` | HTTP 状态服务（见下方路由表） |

### macli server 路由

启动后默认监听 `http://0.0.0.0:8086`，支持浏览器直接访问（返回纯文本）和终端工具访问（返回 ANSI 彩色输出）。

| 路由 | 说明 |
|------|------|
| `GET /gpu` | 所有运行中作业的 GPU / CPU / 内存用量表格，含稳定/临时标记、每卡颜色 |
| `GET /ports` | 所有作业的 SSH 端口映射（JSON） |
| `GET /health` | 服务健康状态，含登录信息、watch / server / autologin 状态（JSON） |
| `GET /log` | macli 主日志最近 1000 行 |
| `GET /watch-log` | watch 定时脚本日志最近 1000 行 |
| `GET /server-log` | HTTP 请求访问日志（含来源 IP） |

`macli server enable --region cn-north-9 --workspace SAI2` 会把 server 的默认
ModelArts 上下文保存到配置中。之后如果 `/gpu` 服务触发自动重新登录，server 会在
重登成功后把 session 恢复到该 region/workspace，避免落到控制台默认区域或默认工作空间。
如果 `/gpu` 显示 `No running jobs`，但 `macli jobs --running` 有结果，优先检查
server 所在机器的 `macli whoami --json` 中 `workspace_id` 是否仍指向目标 workspace。

当工作空间列表或作业列表接口返回 `APIGW.0301 / x-auth-token not found` 时，`macli`
会把它识别为登录态失效并抛出登录异常；启用 `autologin` 后，该异常会交给自动重登流程处理。

## 要求

- Python 3.8+
- macOS / Linux
