# AGENT.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

华为云 ModelArts 远程管理 CLI (`macli`)。通过逆向华为云控制台 REST API 实现训练作业管理、资源监控、远程执行等功能。无官方 SDK 依赖，仅使用 requests + rich。

## Build & Install

```bash
pip install .          # 正式安装
pip install -e .       # 开发模式（需 pip >= 23）
```

安装后通过 `macli` 或 `python -m macli` 运行。入口点：`macli.__main__:main`。

## Architecture

### 模块分层

```
__main__.py          argparse 定义 + dispatch + main() 异常处理/auto-login 重试
    ↓ imports
constants.py         常量、Rich console 单例、SessionExpiredError
config.py            session.json 读写、通用 _load/_save_cfg_section、keyring 凭据
log.py               cprint/dprint/flog 输出层
net.py               _ensure_pkg() 自动依赖安装、_new_session() HTTP 工厂
helpers.py           数据转换、PortCache、SSH 富化、作业过滤/分页
session.py           ConsoleSession（HTTP 会话+认证头）、API（REST 接口封装）
auth.py              登录流程（HTTP MFA）、OTP 轮询、auto-login 熔断器
websocket.py         WebSocket 帧协议、CloudShell 连接
platform_daemon.py   DaemonManager（macOS launchd / Linux pid 统一管理）
commands/            14 个命令模块，每个对应一组 CLI 子命令
```

### 依赖方向（无循环）

`constants` ← `config` ← `log` ← `net` ← `helpers` ← `session` ← `auth`
`websocket` 和 `platform_daemon` 只依赖基础层。`commands/*` 依赖上层库但不互相导入（需要时用 lazy import 避免循环）。

### 关键设计

- **ConsoleSession** 封装带认证的 HTTP 会话，`_checked_request()` 遇到网络异常时自动检测 session 过期并抛 `SessionExpiredError`
- **API** 类是 ModelArts REST API 的薄封装，所有 job CRUD / metrics / exec 操作走这里
- **PortCache** 线程安全的 SSH 端口缓存，Running 作业端口不变，持久化到 session.json
- **DaemonManager** 统一了 watch 和 server 的 macOS launchd / Linux 进程管理代码
- **auto-login**: `main()` 捕获 `SessionExpiredError` → 调用 `_do_auto_login()` → 成功后 `os.execvp` 重新执行原命令
- **server** (`commands/server.py`): 内嵌 FastAPI 应用，通过子进程调用 `macli usage --probe --json` 采集数据，闭包内维护缓存

### 配置存储

所有持久状态存于 `~/.config/macli/session.json`，子域通过 `config.py` 的 `_load_cfg_section(key)` / `_save_cfg_section(key, cfg)` 统一管理。凭据优先存 keyring，不可用时降级为 `credentials.json`（权限 600）。

## Development Notes

- **Python >= 3.8**，运行时依赖仅 `requests` + `rich`（缺失时 `net._ensure_pkg()` 自动安装并重启进程）
- **无测试套件**，无 linter 配置
- `commands/exec_.py` 文件名用下划线后缀避免与 Python 关键字冲突
- `scripts/check_jobs.py` 是 watch daemon 调用的独立脚本，有自己的 argparse，不属于 macli 包

## Remote Deployment

远程华为云容器上部署时：
- 用 `/temp/hanmo/tools/github_ssh_remote.sh pull <repo_dir>` 拉取代码（走 ssh.github.com:443）
- `pip install .` 安装后 `macli server disable && macli server enable` 重载服务
- 不要写全局 git config（共享账号），用 repo-level config

## Huawei Cloud ModelArts 环境

### 账号与容器结构

- **ma-user**：华为云所有训练容器的共享操作系统用户，不同用户的作业跑在同一 uid 下
- 每个训练作业 = 一个 Kubernetes Pod（BestEffort QoS，无 resource requests/limits）
- 所有 keeper 作业（macli watch 创建的保活作业）均为 BestEffort 级别

### 文件系统挂载结构

每个容器内挂载点：

| 挂载点 | 后端存储 | 容量 | 特性 |
|--------|---------|------|------|
| `/` (overlay) | 物理节点 `vgpaas-dockersys` LVM 分区 | 50G/150G/300G（按GPU卡数） | 每容器独立 quota，重启清空 |
| `/cache` | 物理节点 `vgpaas-kubernetes` LVM 分区 | 9.3T（物理节点共享） | 无per-container限额，重启清空 |
| `/temp` | SFS Turbo NFS | 291T（跨所有物理节点共享） | 持久化，多用户共享 |
| `~/modelarts` | `vgpaas-kubernetes` 子目录 | 同 `/cache` 分区 | 与 `/cache` 来自同一物理分区 |

**关键区别**：
- `vgpaas-dockersys` 存储 overlay 层，每容器有独立 prjquota 上限（不能超过自己的配额）
- `vgpaas-kubernetes` 被多个容器通过不同子目录共享，**没有 per-container 限额**

### Kubernetes 驱逐机制

- **触发条件**：`vgpaas-kubernetes` 分区可用空间低于总容量的 **10%**（即 < 946 GiB，总量 9.3T）
- **监控指标**：kubelet 的 `nodefs.available` 信号，监控的是整块物理分区，与单个容器无关
- **驱逐选择**：节点磁盘压力下，kubelet 会综合 Pod 是否超过 request、priority、以及相对 request 的资源使用量选择驱逐对象；ModelArts 训练 Pod 的 ephemeral-storage request 通常为 0，因此高磁盘用量 Pod 风险更高
- `/cache` 无配额保护，任何用户写入大量数据都会威胁整个节点

### 网络暴露

- 训练容器端口无法直接从外部访问
- 当前方案：aliyun-remote 做跳板，通过 SSH 反向隧道将 macli server (8086) 暴露给实验室成员：
  ```
  ssh -N -L 0.0.0.0:8086:localhost:8086 -p <port> ma-user@huaweicloud
  ```
- 实验室成员通过 `http://123.56.30.71:8086/` 访问 dashboard
