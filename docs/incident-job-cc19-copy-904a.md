# 事故分析报告

**Job ID:** `1801fbd6-9f84-4b0f-8c02-e7a6751a19d7`
**Job 名称:** `job-cc19-copy-904a`
**运行时长:** 约 2110 小时（约 88 天）
**最终状态:** Failed
**失败时间:** 2026-04-01 16:41 CST
**分析日期:** 2026-04-02

---

## 一、事件概述

这是一个 ModelArts debug 模式容器（`MA_JOB_RUNTIME_TYPE=debug`），运行 88 天后因 OOM（内存耗尽）引发 K8s 驱逐而终止。

表面原因是 4 月 1 日的 OOM Kill 事件，但深层根因是**从任务启动第一天起 OBS 桶就已写满**，导致所有 core dump 文件无法上传，持续堆积在容器本地存储，88 天后引发存储与内存的双重崩溃。

---

## 二、环境信息

| 项目 | 值 |
|---|---|
| 容器名 | `modelarts-training` |
| Pod 名 | `modelarts-job-1801fbd6-...-worker-0` |
| 节点 IP | `192.168.215.44`（宿主机），`172.16.0.233`（容器内） |
| GPU | 2× NVIDIA（设备 2、6）|
| CUDA | 12.1.0 |
| PyTorch | 2.1.0 |
| 运行模式 | debug（长期占用，sleep 2000000000s 占位）|
| NFS 挂载 | `/temp` → SFS Turbo（持久，重启后保留）|
| OBS 日志目录 | `obs-sai-liush/exp/` |
| 心跳周期 | 600 秒（`MA_TRAINING_CONTAINER_HEARTBEAT_WATCH_PERIOD_SECONDS`）|
| core dump 策略 | `MA_DEFAULT_DUMP_CORE_FILE_TO_OBS=true`（上传 OBS）|

---

## 三、完整时间线

### 阶段一：任务启动，OBS 立即失效（Jan 3）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-01-03 18:19:10 | 容器启动，bootstrap 初始化环境变量，拉起训练进程 | ✅ 已确认（env log）|
| 2026-01-03 18:19:40 | **第一条 OBS 上传失败**（`OBS_ERR_InsufficientStorageSpace`）。OBS 桶在任务启动时就已写满 | ✅ 已确认（output.log）|

**后果：** 此后所有需要写入 OBS 的内容（日志、checkpoint、core dump）全部失败，堆积在容器本地 overlay FS 或 `/cache/`。整个 output.log 文件（2.81 GB，4,262,459 行）中 **99.2% 是这一条错误的重复**。

---

### 阶段二：首次重大崩溃——训练进程 + NCCL watchdog（Jan 9）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-01-09 18:06:43 | `python3.10` 训练进程异常退出，产生 core dump：**7.97 GB** | ✅ 已确认（core 文件名含 Unix 时间戳 1767953203）|
| 同时刻（或极短时间内） | PyTorch NCCL watchdog `pt_nccl_watchdg` 崩溃，core dump：**3.96 GB** | ✅ 已确认（同批次上传失败记录）|
| 同时刻 | 两个 core 文件尝试上传 OBS → 全部失败（OBS 已满）。文件**永久滞留本地** | ✅ 已确认 |

**本地累计 core dump：≈ 11.93 GB**

**待查（实验方向 A）：**
- python3.10 崩溃的具体原因是什么？（SIGABRT / SIGSEGV / SIGBUS？）
- core 文件存放的具体路径是 `/cache/` 还是 overlay FS 的其他目录？

---

### 阶段三：第二次崩溃——node 进程（Feb 7）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-02-07 00:10:42 | `node` 进程崩溃，core dump：**7.8 GB**。OBS 上传失败，滞留本地 | ✅ 已确认（Unix 时间戳 1770394242）|

**本地累计 core dump：≈ 19.73 GB**

**待查（实验方向 B）：**
- 为什么是 `node` 进程？容器中的 node 服务是什么组件？（JupyterLab？VSCode Server？）
- 两次 `node` 崩溃（Feb 7 / Feb 13）是同一根因还是独立事件？

---

### 阶段四：第三次崩溃——node 进程再次崩溃（Feb 13）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-02-13 10:02:27 | `node` 进程再次崩溃，core dump：**6.6 GB**。上传失败，滞留本地 | ✅ 已确认（Unix 时间戳 1770948147）|

**本地累计 core dump：≈ 26.33 GB**

---

### 阶段五：平静期（Feb 13 → Apr 1，约 47 天）

容器表面上仍在运行。OBS 错误持续刷日志，无其他异常记录。

**实际发生的事（推测，待查）：**
- 26 GB core dump 持续占用本地存储，从未被清理
- 若新的训练进程在此期间启动，其内存占用在原有高水位基础上叠加
- 内存可能多次接近 cgroup 上限又回落，直到 4 月 1 日最终突破

**待查（实验方向 C）：**
- 这 47 天内是否有更多 OOM kill 事件？（宿主机 dmesg 日志，容器内不可见）
- 训练进程在 Jan 9 崩溃后是否被重新拉起？（bootstrap 的 retry 逻辑）

---

### 阶段六：OOM 级联崩溃（Apr 1）

#### 03:52 — 第一波 OOM（进程级）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-04-01 03:52 | 宿主机 OOM killer 介入，kill 了容器内某个进程 | ✅ 已确认（diagnosis.json MDC.300710 第一条记录）|
| 03:52 | kill 后内存临时释放，容器继续运行 | ✅ 推断（容器在此后数小时仍存活）|

**待查（实验方向 D）：**
- 03:52 具体 kill 的是哪个进程？（需宿主机 `dmesg | grep "Killed process"`）
- kill 后释放了多少内存？（需宿主机 `/sys/fs/cgroup/memory/memory.usage_in_bytes` 历史数据）

#### 09:35 — metrics-collect 被 OOM kill（第二波）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-04-01 09:35 | `metrics-collect`（ModelArts 平台资源监控 agent）被 OOM killer 杀死 | ✅ 已确认（diagnosis.json MDC.300710 直接记录）|
| 09:35 | `metrics-collect` RSS：**495 MB** | ✅ 已确认（diagnosis.json）|
| 09:35 | 平台**失去对该容器的资源监控能力**（相当于拔掉报警器）| ✅ 推断 |

**为什么 kill 掉 495MB 还不够：**
495MB 的释放只是暂时的。内存水位线过高的根本原因是其他进程（训练主进程或 sidecar）持续占用大量内存，单次释放无法将总占用降到安全水位以下。

**待查（实验方向 E）：**
- 09:35 时的内存总占用是多少？（需 cgroup 历史数据）
- 03:52 到 09:35 之间有没有其他进程被 kill？

#### 09:35 → 15:36 — 6 小时挣扎期（黑盒）

这 6 小时内没有容器可读的日志产出。

**推测发生的事：**
- OOM killer 可能在此期间多次 kill 用户进程
- 每次 kill 短暂释放内存，但主力内存消耗进程（训练进程或其重启版本）很快恢复占用
- 内存水位在 kill → 回升 → kill → 回升的循环中缓慢攀升至 cgroup 上限

**待查（实验方向 F）：**
- 这 6 小时内的内存曲线（需 Prometheus/CES 指标数据，`MA_CUSTOM_METRICS_EXPORTER_MAOS_AGENT_ON=true` 说明指标上报是开启的）
- 可通过华为云 CES 控制台查询该时间段的容器内存监控

#### 15:36 — Container OOMKilled（容器级）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-04-01 15:36 | 容器整体内存超过 cgroup 限制，K8s 将容器标记为 `OOMKilled` | ✅ 已确认（diagnosis.json MDC.300710）|

**这一步的含义：** 不再是 OOM killer 在容器内挑选单个进程，而是 cgroup 内存总用量突破上限，内核击穿整个 memory cgroup，K8s 接管并终止容器。

#### 16:27 — Ephemeral Storage Eviction（磁盘驱逐）

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-04-01 16:27 | K8s kubelet 检测到节点临时存储告急 | ✅ 已确认（diagnosis.json MDC.100050）|
| 16:27 | 节点可用临时存储：**945.8 GB**，驱逐阈值：**946.5 GB**（差距仅 **0.7 GB**）| ✅ 已确认 |
| 16:27 | Sidecar 容器使用临时存储 **2.4 GB**（request 为 0，超出节点容忍上限）| ✅ 已确认 |

**待查（实验方向 G）：**
- 这 2.4 GB sidecar 存储是什么内容？是 core dump 溢出到 overlay FS？还是 sidecar 自身的日志/缓存？
- 26.3 GB core dump 文件最终位于节点的哪个路径？容器重启后是否已消失？

#### 16:41 — Bootstrap ECHILD，任务宣告失败

| 时间 | 事件 | 状态 |
|---|---|---|
| 2026-04-01 16:41 | ModelArts bootstrap 调用 `wait()` 返回 **`ECHILD`**（"没有子进程"）| ✅ 已确认（output.log 最后条目）|
| 16:41 | Bootstrap 判断监控目标进程（PID 89）已死亡，上报任务失败 | ✅ 已确认 |
| 16:41 | Job 状态变为 `Failed` | ✅ 已确认 |

**PID 89 是谁：**
`ECHILD` 说明 PID 89 在 bootstrap 等待之前就已死亡，且其退出状态已被消费（可能由 OOMKill 触发）。PID 89 大概率是任务的主占位进程（`sleep 2000000000s`）或训练主进程。

**待查（实验方向 H）：**
- PID 89 的具体身份（`/proc/89/comm` 在容器存活时可查，但现在容器已终止）

---

## 四、因果链

```
根因 1：OBS 桶满（任务启动时已存在）
    ↓
所有 core dump 无法上传，堆积本地

根因 2：训练代码存在多处崩溃（1 月、2 月各 2-3 次）
    ↓
每次崩溃产生数 GB 的 core dump 文件

两个根因叠加：
    ↓
88 天内累计 ≈26.3 GB core dump 滞留本地 overlay FS / /cache/
    ↓
本地磁盘压力上升（26.3 GB 非常接近节点 0.7 GB 的余量告警线）
内存水位线持续偏高（core dump 相关进程、内存碎片、训练进程未释放内存等）
    ↓
Apr 1 03:52  第一次 OOM kill（用户进程）
Apr 1 09:35  metrics-collect（495MB）OOM kill → 监控盲区
Apr 1 09:35–15:36  内存 kill-回升循环，水位继续上升
Apr 1 15:36  cgroup 内存上限被突破 → Container OOMKilled
Apr 1 16:27  节点临时存储告急 → K8s Eviction（雪上加霜）
Apr 1 16:41  bootstrap ECHILD → Job Failed
```

---

## 五、已确认的事实 vs. 待查细节汇总

### ✅ 已确认

| # | 事实 | 信息来源 |
|---|---|---|
| 1 | OBS 桶在任务启动时（Jan 3）就已写满 | output.log 第一条 OBS 错误时间戳 |
| 2 | Jan 9 python3.10 崩溃，产生 7.97 GB core dump | output.log core 文件上传失败记录 |
| 3 | Jan 9 pt_nccl_watchdg 崩溃，产生 3.96 GB core dump | output.log core 文件上传失败记录 |
| 4 | Feb 7 node 崩溃，产生 7.8 GB core dump | output.log core 文件上传失败记录 |
| 5 | Feb 13 node 崩溃，产生 6.6 GB core dump | output.log core 文件上传失败记录 |
| 6 | 4 个 core dump 文件共约 26.3 GB，全部未能上传 OBS | output.log |
| 7 | Apr 1 09:35 metrics-collect（495MB）被 OOM kill | diagnosis.json MDC.300710 |
| 8 | Apr 1 15:36 Container OOMKilled | diagnosis.json MDC.300710 |
| 9 | Apr 1 16:27 节点临时存储驱逐，余量 945.8 GB < 阈值 946.5 GB | diagnosis.json MDC.100050 |
| 10 | Apr 1 16:41 bootstrap ECHILD，任务 Failed | output.log 最后条目 |
| 11 | OBS 桶 InsufficientStorageSpace 贯穿全程 | diagnosis.json MDC.300450 |

### ❓ 待查清

| # | 问题 | 重要性 | 实验/查询方向 |
|---|---|---|---|
| A | 容器 cgroup 内存上限是多少？ | 高 | 华为云控制台 → 训练作业 → 资源规格；或 K8s API `kubectl get pod ... -o yaml` |
| B | Apr 1 03:52 被 kill 的进程是谁？kill 后内存释放了多少？ | 高 | 宿主机 `dmesg` 日志（需联系华为云技术支持）|
| C | core dump 文件存储在容器的哪个路径（`/cache/` or overlay FS）？ | 高 | 在新容器中执行 `ulimit -c unlimited` 触发一次崩溃，观察 core 文件位置 |
| D | Jan 9 python3.10 崩溃的具体原因（信号类型、调用栈）？ | 中 | 需要 core dump 文件 + gdb 分析；或查看 NCCL 日志 |
| E | 容器内存在 Apr 1 全天的变化曲线 | 高 | 华为云 CES（`MA_CUSTOM_METRICS_EXPORTER_MAOS_AGENT_ON=true` 已开启指标上报）|
| F | 训练进程在 Jan 9 崩溃后是否被自动重启？重启了几次？ | 中 | bootstrap 的 retry 逻辑；检查 output.log 中的进程重启日志 |
| G | 容器内的 `node` 进程是什么服务？为什么会两次崩溃？ | 中 | debug 容器内 `ps aux` / `which node` / JupyterLab 日志 |
| H | PID 89 的具体身份 | 低 | 容器存活时 `/proc/89/comm`；已终止则无法获取 |
| I | 26.3 GB core dump 是否导致了 Apr 1 的节点磁盘告警（MDC.100050）？ | 高 | 重现实验：在 debug 容器中产生大 core dump，观察节点临时存储变化 |

---

## 六、预防措施建议

### 立即可执行

1. **清理/限制 core dump 大小**（在 debug 容器启动命令中加入）：
   ```bash
   ulimit -c 0            # 完全禁用 core dump
   # 或
   ulimit -c 1048576      # 限制为最大 1GB（单位：512B blocks → 1GB = 2097152）
   echo "ulimit -c 0" >> ~/.bashrc
   ```

2. **使用 container_watchdog.py**（`scripts/container_watchdog.py`）：
   - 监控 `/cache/` 磁盘用量，超过 90% 时主动 kill 最大写入进程
   - 监控内存 cgroup，超过 85% 时主动 kill 最大 RSS 进程
   - 可在 debug 容器启动时自动运行：
     ```bash
     nohup python3 /temp/container_watchdog.py >> /tmp/watchdog.log 2>&1 &
     ```

3. **任务启动前检查 OBS 余量**：确认 OBS 桶有足够空间再启动长期 debug 任务。

### 深层修复

4. **定期清理 core dump 文件**：在训练脚本中加入 cron 或后台进程，定期删除超过 N 天或超过 X GB 的 core 文件：
   ```bash
   find /cache/ -name "core.*" -mtime +3 -delete
   ```

5. **OBS 桶容量监控告警**：在华为云 CES 配置 OBS 桶容量告警，接近上限前提前通知。

6. **查询 CES 内存指标**：长期 debug 任务定期检查容器内存曲线，在接近 OOM 前主动介入。

---

## 七、复现实验设计建议

针对待查细节，建议设计以下实验：

### 实验 1：确认 core dump 的存储位置（对应待查 C、I）

**目标：** 搞清楚 core dump 文件写到哪里，以及是否计入节点临时存储配额。

**步骤：**
1. 启动一个 ModelArts debug 容器
2. 不加 `ulimit -c 0`，执行 `sleep 1 & kill -SIGSEGV $!`（触发小 core dump）
3. 观察 core 文件出现在哪个路径
4. 用 `df -h` 确认哪个挂载点的用量增加
5. 在宿主机侧（需 K8s 权限）查看 `kubectl describe pod` 的 `ephemeral-storage` 用量变化

### 实验 2：内存水位逼近 cgroup 上限时的行为（对应待查 A、B、E）

**目标：** 复现 OOM kill 事件，确认哪个进程先被 kill，kill 顺序是否可预测。

**步骤：**
1. 在 debug 容器中启动多个进程（分别占用不同大小内存）
2. 持续分配内存，逼近 cgroup 限制
3. 观察 OOM killer 的 kill 顺序（按 oom_score 排序）
4. 确认 metrics-collect 等 sidecar 进程的 oom_score 值

### 实验 3：container_watchdog.py 的有效性验证（对应预防措施）

**目标：** 验证 watchdog 能否在 OOM 之前正确 kill 目标进程并写入告警。

**步骤：**
1. 在 debug 容器中以 `WATCHDOG_MEM_PCT=50` 启动 watchdog（降低阈值以便测试）
2. 启动一个持续占用内存的进程
3. 观察 watchdog 是否在阈值触发前正确发送 SIGTERM/SIGKILL
4. 确认 `/temp/WATCHDOG_ALERT.txt` 是否正确写入

---

*本报告基于 `output.log`（2.81 GB）和 `diagnosis.json` 的分析，以及 ModelArts bootstrap 日志和容器环境变量。宿主机级别的信息（dmesg、cgroup 历史数据）不在可访问范围内，标注为待查。*
