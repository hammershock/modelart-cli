# macli

华为云 ModelArts 远程管理 CLI

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
macli server enable
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
| `autologin` | 会话过期自动重登（ntfy 推送验证码） |
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

## 要求

- Python 3.8+
- macOS / Linux
