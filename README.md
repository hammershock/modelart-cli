# macli

华为云 ModelArts 远程管理 CLI

## 安装

```bash
pip install git+https://github.com/hammershock/modelart-cli.git
```

## 快速开始

```bash
macli login                     # 登录
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
| `watch` | 定时保活 + 清理 Terminated 作业（launchd） |
| `server` | HTTP 状态服务，提供 `/gpu` `/ports` `/health` 接口 |

## 要求

- Python 3.8+
- macOS / Linux
