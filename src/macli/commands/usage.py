"""usage 命令：作业资源使用率监控（API 时序 + probe 实时采样）"""
import os, sys, time
import concurrent.futures
from datetime import datetime

from macli.constants import _CST, SessionExpiredError, console, Progress, TextColumn, BarColumn
from macli.config import get_exec_backend
from macli.log import cprint, dprint, _raw_debug
from macli.helpers import (PortCache, resolve_ssh, ssh_ports_summary,
                           ms_to_hms, ts_to_str, _json_out, _fetch_all_jobs,
                           build_quota_annotations)
from macli.session import _sess_or_exit, API
from macli.commands.log_cmd import _pick_log_task
from rich.panel import Panel
from rich.table import Table


def _build_usage_query(metric_name: str, job_id: str, window_ms: int = 59999) -> str:
    return (
        f'avg(label_replace(avg_over_time({metric_name}'
        f'{{service_id="{job_id}",container_name="modelarts-training"}}'
        f'[{window_ms}ms]),"__name__","{metric_name}","",""))by(__name__,service_id)'
    )


def _usage_series_stats(values: list) -> dict:
    pairs = []
    for item in values or []:
        try:
            ts = int(item[0])
            val = float(item[1])
            pairs.append((ts, val))
        except Exception:
            continue
    if not pairs:
        return {"count": 0, "latest": None, "avg": None, "max": None}
    only_vals = [v for _, v in pairs]
    return {
        "count": len(pairs),
        "latest": pairs[-1][1],
        "avg": sum(only_vals) / len(only_vals),
        "max": max(only_vals),
        "start": pairs[0][0],
        "end": pairs[-1][0],
        "values": pairs,
    }


def _fmt_usage_value(metric_key: str, val):
    if val is None:
        return "--"
    if metric_key in {"cpu_util", "memory_util", "gpu_util", "gpu_mem_util"}:
        return f"{val * 100:.2f}%"
    if metric_key == "cpu_used_core":
        return f"{val:.4f} cores"
    if metric_key in {"memory_used_megabytes", "gpu_mem_used_megabytes"}:
        return f"{val:.2f} MB"
    return str(val)


def _sparkline(values: list, width: int = 32) -> str:
    ticks = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"
    vals = []
    for item in values or []:
        try:
            vals.append(float(item[1] if isinstance(item, (list, tuple)) and len(item) >= 2 else item))
        except Exception:
            continue
    if not vals:
        return "\u00b7" * width
    if len(vals) > width:
        step = len(vals) / width
        sampled = []
        for i in range(width):
            idx = min(int(i * step), len(vals) - 1)
            sampled.append(vals[idx])
        vals = sampled
    elif len(vals) < width:
        vals = [vals[0]] * (width - len(vals)) + vals
    vmin, vmax = min(vals), max(vals)
    if vmax - vmin < 1e-12:
        level = 0 if vmax <= 0 else len(ticks) // 2
        return ticks[level] * len(vals)
    chars = []
    for v in vals:
        idx = int(round((v - vmin) / (vmax - vmin) * (len(ticks) - 1)))
        idx = max(0, min(len(ticks) - 1, idx))
        chars.append(ticks[idx])
    return "".join(chars)


_USAGE_METRIC_ALIASES = {
    "cpu":  "cpu",
    "mem":  "mem",  "memory": "mem",
    "gpu":  "gpu",
    "vram": "vram", "gpu_mem": "vram", "gmem": "vram",
}

def _parse_metrics_filter(raw: list) -> set:
    """将用户传入的 metric 名列表规范化为内部 key 集合（cpu/mem/gpu/vram）。"""
    result = set()
    for item in raw or []:
        key = _USAGE_METRIC_ALIASES.get(item.lower())
        if key:
            result.add(key)
    return result or {"cpu", "mem", "gpu", "vram"}   # 空 = 全部


def _util_bar(util: float, width: int = 36) -> str:
    """探针模式用：单采样点实心进度条（无时序数据，不用 sparkline）。"""
    filled = round(max(0.0, min(1.0, util)) * width)
    return "\u2588" * filled + "\u2591" * (width - filled)


def _usage_panel_text(result: dict, filter_set: set = None) -> str:
    m        = result["metrics"]
    is_probe = result.get("probe", False)
    # 每卡详情（probe 模式专有）
    gpu_devs  = m.get("gpu_devices", []) if is_probe else []
    want_gpu  = filter_set is None or "gpu"  in filter_set
    want_vram = filter_set is None or "vram" in filter_set
    use_per_gpu = bool(gpu_devs) and (want_gpu or want_vram)

    lines = []
    all_rows = [
        ("CPU",  "cpu",  "cpu_util",     "cpu_used_core"),
        ("\u5185\u5b58", "mem",  "memory_util",  "memory_used_megabytes"),
        ("GPU",  "gpu",  "gpu_util",     "gpu_mem_used_megabytes"),
        ("\u663e\u5b58", "vram", "gpu_mem_util", "gpu_mem_used_megabytes"),
    ]
    # 有每卡数据时跳过通用 GPU/VRAM 行，改用下方的 per-GPU 渲染
    rows = [(t, uk, sk) for t, k, uk, sk in all_rows
            if (filter_set is None or k in filter_set)
            and not (use_per_gpu and k in {"gpu", "vram"})]

    start_ts = None
    end_ts   = None
    for title, util_key, used_key in rows:
        util = m.get(util_key, {})
        used = m.get(used_key, {})
        if start_ts is None and util.get("start"):
            start_ts = util.get("start")
        if util.get("end"):
            end_ts = util.get("end")
        latest_util = util.get("latest")
        percent = f"{latest_util * 100:.1f}%" if latest_util is not None else "--"
        latest_used = _fmt_usage_value(used_key, used.get("latest"))
        avg_used    = _fmt_usage_value(used_key, used.get("avg"))
        if is_probe:
            bar = _util_bar(latest_util or 0)
        else:
            bar = _sparkline(util.get("values", []), width=36)
        color = 'green' if (latest_util or 0) < 0.5 else 'yellow' if (latest_util or 0) < 0.8 else 'red'
        lines.append(f"[bold]{title:<4}[/bold] [{color}]{bar}[/{color}] {percent}")
        if is_probe:
            # 单点采样，latest==avg，只显示一行值
            if used.get("latest") is not None:
                lines.append(f"      {latest_used}")
        else:
            lines.append(f"      latest={latest_used}   avg={avg_used}")

    # -- 每卡 GPU/VRAM 行（probe 模式）--
    for dev in gpu_devs:
        idx        = dev["index"]
        util       = dev.get("util",         0.0)
        vram_used  = dev.get("vram_used_mb",  0.0)
        vram_total = dev.get("vram_total_mb", 0.0)
        vram_util  = dev.get("vram_util",     0.0)

        color = 'green' if util < 0.5 else 'yellow' if util < 0.8 else 'red'

        if want_gpu:
            bar = _util_bar(util)
            lines.append(f"[bold]GPU{idx} [/bold][{color}]{bar}[/{color}] {util*100:.1f}%")
        if want_vram:
            vram_bar   = _util_bar(vram_util)
            vram_str   = f"{vram_used:.0f}/{vram_total:.0f} MB"
            vram_color = 'green' if vram_util < 0.5 else 'yellow' if vram_util < 0.8 else 'red'
            prefix = "     " if want_gpu else f"[bold]GPU{idx} [/bold]"
            lines.append(f"{prefix}[{vram_color}]{vram_bar}[/{vram_color}] VRAM {vram_str}  {vram_util*100:.1f}%")

    if start_ts and end_ts:
        st = datetime.fromtimestamp(start_ts, tz=_CST).strftime('%H:%M')
        ed = datetime.fromtimestamp(end_ts, tz=_CST).strftime('%H:%M')
        lines.append("")
        lines.append(f"[dim]{st} {'─' * 34} {ed}[/dim]")
    return "\n".join(lines)


def _fetch_usage_result(api: API, job_id: str, minutes: int, step: int) -> dict:
    end_ts = int(time.time())
    start_ts = end_ts - int(minutes) * 60
    metrics = {
        "cpu_used_core": "ma_container_cpu_used_core",
        "cpu_util": "ma_container_cpu_util",
        "memory_used_megabytes": "ma_container_memory_used_megabytes",
        "memory_util": "ma_container_memory_util",
        "gpu_util": "ma_container_gpu_util",
        "gpu_mem_used_megabytes": "ma_container_gpu_mem_used_megabytes",
        "gpu_mem_util": "ma_container_gpu_mem_util",
    }
    result = {
        "job_id": job_id,
        "minutes": minutes,
        "step": step,
        "metrics": {},
    }
    # 监控 API 的 util 类指标以百分比（0-100）返回，统一除以 100 归一化为 0-1
    # 以便与 probe 模式、_usage_panel_text 里的 x100 显示逻辑保持一致
    _API_PERCENT_KEYS = {"cpu_util", "memory_util", "gpu_util", "gpu_mem_util"}

    for key, metric_name in metrics.items():
        query = _build_usage_query(metric_name, job_id)
        data = api.query_usage_range(query=query, start=start_ts, end=end_ts, step=step)
        series = (((data or {}).get("data") or {}).get("result") or [])
        values = (series[0].get("values") if series else []) or []
        stats = _usage_series_stats(values)
        if key in _API_PERCENT_KEYS:
            for field in ("latest", "avg", "max"):
                if stats[field] is not None:
                    stats[field] = stats[field] / 100
            stats["values"] = [[ts, v / 100] for ts, v in stats["values"]]
        result["metrics"][key] = stats
    return result


# -- Probe system (CloudShell-based remote metric collection) --
#
# 每个 ProbeSpec 封装一个资源维度的探测：shell 脚本 + 输出解析。
# 所有激活的探针合并进一个脚本，通过单次 exec 连接完成采集。
# 新增平台/指标只需在 _PROBE_REGISTRY 里追加新条目即可。

def _probe_kv(text: str) -> dict:
    """解析 'key=value' 行，值转 float（失败则跳过）。"""
    result = {}
    for line in text.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            try:
                result[k.strip()] = float(v.strip())
            except ValueError:
                pass
    return result


def _probe_metric(val) -> dict:
    """将单个采样值包装成与 _usage_series_stats 兼容的 metrics 格式。"""
    if val is None:
        return {"count": 0, "latest": None, "avg": None, "max": None, "values": []}
    v = float(val)
    return {"count": 1, "latest": v, "avg": v, "max": v, "values": []}


class _ProbeSpec:
    """单个探针规格：探测哪些 filter_keys、执行什么 shell、如何解析输出。"""
    def __init__(self, key: str, filter_keys, shell: str, parse_fn):
        self.key         = key
        self.filter_keys = frozenset(filter_keys)
        self.shell       = shell.strip()
        self.parse_fn    = parse_fn   # (kv: dict) -> {metric_key: metric_dict}


# -- 各平台/指标的探针定义 --

def _probe_parse_system(kv: dict) -> dict:
    """解析 top -bn2 输出的 CPU + 内存数据。"""
    return {
        "cpu_util":              _probe_metric(kv.get("cpu_util")),
        "cpu_used_core":         _probe_metric(None),
        "memory_util":           _probe_metric(kv.get("mem_util")),
        "memory_used_megabytes": _probe_metric(kv.get("mem_used_mb")),
    }

def _probe_parse_gpu(kv: dict) -> dict:
    gpu_count = int(kv.get("gpu_count", 0))
    devices = []
    for i in range(gpu_count):
        util       = kv.get(f"gpu_{i}_util")
        vram_used  = kv.get(f"gpu_{i}_vram_used_mb")
        vram_total = kv.get(f"gpu_{i}_vram_total_mb")
        if util is not None:
            vram_util = (vram_used / vram_total) if (vram_total and vram_total > 0) else 0.0
            devices.append({
                "index":        i,
                "util":         util,
                "vram_used_mb": vram_used  or 0.0,
                "vram_total_mb":vram_total or 0.0,
                "vram_util":    vram_util,
            })
    return {
        "gpu_util":               _probe_metric(kv.get("gpu_avg_util")),
        "gpu_mem_util":           _probe_metric(kv.get("vram_avg_util")),
        "gpu_mem_used_megabytes": _probe_metric(kv.get("vram_avg_used_mb")),
        "gpu_devices":            devices,   # list[dict], probe-only per-GPU detail
    }


_PROBE_REGISTRY: "list[_ProbeSpec]" = [
    _ProbeSpec(
        key="system",
        filter_keys={"cpu", "mem"},
        # CPU：cpuacct.usage 两次采样 delta / (interval_ns x 分配核数) = 容器级利用率
        # 内存：cgroup usage_in_bytes - page cache = RSS，相对 limit 计算利用率
        # 均读自 cgroup v1，不受 host /proc 污染
        shell=r"""
_quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
_period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
_ncpu=$((_quota / _period))
_t1=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
sleep 1
_t2=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
_delta=$((_t2 - _t1))
awk -v d=$_delta -v n=$_ncpu 'BEGIN{printf "cpu_util=%.6f\n", d/(1e9*n)}'

_usage=$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes)
_limit=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
_cache=$(grep "^cache " /sys/fs/cgroup/memory/memory.stat | awk '{print $2}')
_rss=$((_usage - _cache))
awk -v u=$_rss -v l=$_limit 'BEGIN{printf "mem_used_mb=%.2f\nmem_util=%.6f\n", u/1048576, (l>0)?u/l:0}'
""",
        parse_fn=_probe_parse_system,
    ),
    _ProbeSpec(
        key="gpu",
        filter_keys={"gpu", "vram"},
        # 每卡输出 gpu_N_util / gpu_N_vram_used_mb / gpu_N_vram_total_mb
        # 同时输出聚合均值供回退使用；nvidia-smi 不可用时全零
        shell=r"""
if command -v nvidia-smi &>/dev/null; then
    nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total \
        --format=csv,noheader,nounits 2>/dev/null | \
    awk -F'[, ]+' '{
        i=$1; gu=$2; vm=$3; vt=$4
        printf "gpu_%d_util=%.6f\ngpu_%d_vram_used_mb=%.2f\ngpu_%d_vram_total_mb=%.2f\n",
               i,gu/100, i,vm, i,vt
        tgu+=gu; tvm+=vm; tvt+=vt; n++
    } END{
        if(n>0) printf "gpu_count=%d\ngpu_avg_util=%.6f\nvram_avg_used_mb=%.2f\nvram_avg_util=%.6f\n",
                        n, tgu/100/n, tvm/n, (tvt>0)?tvm/tvt:0
        else    print "gpu_count=0\ngpu_avg_util=0\nvram_avg_used_mb=0\nvram_avg_util=0"
    }'
else
    printf "gpu_count=0\ngpu_avg_util=0\nvram_avg_used_mb=0\nvram_avg_util=0\n"
fi
""",
        parse_fn=_probe_parse_gpu,
    ),
]


def _run_probes(
    sess: "ConsoleSession",
    job_id: str,
    task_name: str,
    filter_set: set,
    timeout: int = 60,
    backend: str = "cloudshell",
    ssh_entries: list = None,
    identityfile: str = None,
    ssh_opts: list = None,
) -> dict:
    """
    将 filter_set 对应的所有探针合并为一个脚本，经单次 exec 连接执行，
    返回与 _fetch_usage_result 格式兼容的 result dict。
    backend 可为 "cloudshell"（默认）或 "ssh"。
    """
    from macli.commands.exec_ import _exec_script, _exec_script_ssh_capture

    active = [p for p in _PROBE_REGISTRY if p.filter_keys & filter_set]
    if not active:
        return {"job_id": job_id, "probe": True, "metrics": {}}

    PSTART = "MACLI_PROBE_START"
    PEND   = "MACLI_PROBE_END"

    script_parts = ["#!/bin/bash"]
    for p in active:
        script_parts.append(f'echo "{PSTART}:{p.key}"')
        script_parts.append(p.shell)
        script_parts.append(f'echo "{PEND}:{p.key}"')
    script = "\n".join(script_parts)

    dprint(f"[dim]probe: 运行 {[p.key for p in active]} (单次连接, 后端={backend})[/dim]")

    def _attempt():
        if backend == "ssh":
            output, _ = _exec_script_ssh_capture(
                ssh_entries or [], script, task=task_name, timeout=timeout,
                identityfile=identityfile, ssh_opts=ssh_opts,
            )
        else:
            output, _ = _exec_script(sess, job_id, task_name, script, timeout=timeout)
        _raw_debug(f"probe raw output:\n{output}")
        found_any = any(f"{PSTART}:{p.key}" in output for p in active)
        m: dict = {}
        for p in active:
            section = ""
            s_mark = f"{PSTART}:{p.key}"
            e_mark = f"{PEND}:{p.key}"
            if s_mark in output:
                after = output.split(s_mark, 1)[1]
                section = after.split(e_mark, 1)[0] if e_mark in after else after
            kv = _probe_kv(section)
            dprint(f"[dim]probe [{p.key}] kv={kv}[/dim]")
            m.update(p.parse_fn(kv))
        return m, found_any

    t0 = time.monotonic()
    metrics, found = _attempt()
    if not found:
        dprint("[dim]probe: 未收到输出，重试中 (1/2)...[/dim]")
        metrics, found = _attempt()
    if not found:
        dprint("[dim]probe: 未收到输出，重试中 (2/2)...[/dim]")
        metrics, _ = _attempt()
    elapsed = time.monotonic() - t0

    return {"job_id": job_id, "probe": True, "probe_backend": backend,
            "probe_elapsed_s": round(elapsed, 2), "metrics": metrics}


def _usage_check_exec_access(api: "API", job_id: str, preferred_task: str = None) -> str:
    """检查 CloudShell 权限并返回 task_name，失败则 exit。"""
    status = api.get_exec_status(job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业 CloudShell 未就绪，无法使用 --probe[/red]")
            sys.exit(1)
    tasks = api.get_job_tasks(job_id)
    return _pick_log_task(tasks, preferred=preferred_task)


def cmd_usage(args):
    sess = _sess_or_exit()
    api  = API(sess)

    use_probe     = getattr(args, "probe", False)
    probe_backend = get_exec_backend() if use_probe else "cloudshell"
    filter_set    = _parse_metrics_filter(getattr(args, "metrics", None) or [])

    if args.job_id:
        if use_probe:
            if probe_backend == "ssh":
                port_cache = PortCache().load()
                try:
                    job_detail = api.get_job(args.job_id)
                    if not job_detail: sys.exit(1)
                    phase = job_detail.get("status", {}).get("phase", "")
                    probe_ssh_entries = resolve_ssh(api, args.job_id, phase, port_cache,
                                                    detail_hint=job_detail)
                    port_cache.save()
                except SessionExpiredError:
                    if os.environ.get("MACLI_NO_AUTOLOGIN"):
                        raise
                    cprint("[yellow]WARN: session 已失效，使用缓存 SSH 端口进行探测[/yellow]")
                    probe_ssh_entries = port_cache.get(args.job_id)
                if not probe_ssh_entries:
                    cprint("[red]该作业暂无 SSH 信息，无法使用 SSH 后端 probe[/red]"); sys.exit(1)
                preferred = getattr(args, "task", None)
                task_name = preferred or probe_ssh_entries[0]["task"]
            else:
                probe_ssh_entries = None
                task_name = _usage_check_exec_access(api, args.job_id,
                                                     preferred_task=getattr(args, "task", None))
            result = _run_probes(sess, args.job_id, task_name, filter_set,
                                 timeout=getattr(args, "timeout", 60),
                                 backend=probe_backend, ssh_entries=probe_ssh_entries)
        else:
            result = _fetch_usage_result(api, args.job_id, args.minutes, args.step)
        if getattr(args, "json", False):
            _json_out(result)
            return
        console.print(Panel(
            _usage_panel_text(result, filter_set=filter_set),
            title=f"\u4f5c\u4e1a\u76d1\u63a7  {args.job_id}",
            border_style="cyan",
        ))
        if use_probe:
            pb = result.get("probe_backend", "cloudshell")
            el = result.get("probe_elapsed_s")
            el_str = f"  耗时 {el}s" if el is not None else ""
            cprint(f"[dim][probe] 实时单点采样  后端={pb}{el_str}[/dim]")
        else:
            cprint(f"[dim]时间范围: 最近 {args.minutes} 分钟，step={args.step}s[/dim]")
        return

    port_cache  = PortCache().load()
    concurrency = getattr(args, "concurrency", 8)
    # degraded[0] = True when session is expired; set on first SessionExpiredError
    degraded = [False]

    try:
        all_jobs = _fetch_all_jobs(api)
        running_jobs = [j for j in all_jobs if j.get("status", {}).get("phase") == "Running"]
        port_cache.evict_non_running({j.get("metadata", {}).get("id", "")
                                       for j in running_jobs
                                       if j.get("metadata", {}).get("id")})
    except SessionExpiredError:
        if use_probe and probe_backend == "ssh" and not os.environ.get("MACLI_NO_AUTOLOGIN"):
            cprint("[yellow]WARN: session 已失效，使用缓存 SSH 端口进行探测[/yellow]")
            degraded[0] = True
            running_jobs = [
                {"metadata": {"id": jid, "name": jid[:8]}, "status": {"phase": "Running"}}
                for jid in port_cache._data
            ]
        else:
            raise

    def _fetch_one(job):
        meta        = job.get("metadata", {})
        st          = job.get("status",   {})
        res         = job.get("spec", {}).get("resource", {})
        pool_info   = res.get("pool_info", {})
        job_id      = meta.get("id", "")
        name        = meta.get("name", "")
        create_time = meta.get("create_time")          # ms timestamp
        duration_ms = st.get("duration")               # ms
        try:
            if use_probe:
                if probe_backend == "ssh":
                    if not degraded[0]:
                        try:
                            job_detail = api.get_job(job_id)
                            if job_detail:
                                phase_p = job_detail.get("status", {}).get("phase", "")
                                probe_ssh_entries = resolve_ssh(api, job_id, phase_p,
                                                                port_cache, detail_hint=job_detail)
                            else:
                                probe_ssh_entries = []
                        except SessionExpiredError:
                            degraded[0] = True
                            probe_ssh_entries = port_cache.get(job_id) or []
                    else:
                        probe_ssh_entries = port_cache.get(job_id) or []
                    preferred = getattr(args, "task", None)
                    task_name = preferred or (probe_ssh_entries[0]["task"] if probe_ssh_entries else "worker-0")
                else:
                    probe_ssh_entries = None
                    task_name = _usage_check_exec_access(api, job_id,
                                                         preferred_task=getattr(args, "task", None))
                u = _run_probes(sess, job_id, task_name, filter_set,
                                timeout=getattr(args, "timeout", 60),
                                backend=probe_backend, ssh_entries=probe_ssh_entries)
            else:
                u = _fetch_usage_result(api, job_id, args.minutes, args.step)
        except Exception as e:
            dprint(f"[red]{job_id} 采集失败: {e}[/red]")
            u = {"metrics": {}}

        # SSH 端口：从共享 PortCache 读取（probe SSH 模式下 resolve_ssh 已更新缓存）
        port_entries = port_cache.get(job_id) or []
        ssh_port = ssh_ports_summary(port_entries)

        return {
            "job_id":       job_id,
            "name":         name,
            "status":       st.get("phase", "Running"),
            "pool_id":      res.get("pool_id") or pool_info.get("pool_id", ""),
            "gpu_count":    pool_info.get("accelerator_num") or 1,
            "ssh_port":     ssh_port,
            "create_time":  int(create_time) if create_time is not None else 0,
            "duration_ms":  int(duration_ms) if duration_ms is not None else 0,
            "collected_at": datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S"),
            "cpu":          u["metrics"].get("cpu_util",              {}).get("latest"),
            "mem":          u["metrics"].get("memory_used_megabytes", {}).get("latest"),
            "gpu":          u["metrics"].get("gpu_util",              {}).get("latest"),
            "gpu_mem":      u["metrics"].get("gpu_mem_used_megabytes",{}).get("latest"),
            "gpu_devices":  u["metrics"].get("gpu_devices", []),
        }

    rows_map = {}
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
        transient=True,
    ) as progress:
        ptask = progress.add_task(
            f"\u91c7\u96c6\u4e2d\uff08\u5e76\u53d1={concurrency}\uff09...", total=len(running_jobs)
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            fut_to_idx = {pool.submit(_fetch_one, job): i
                          for i, job in enumerate(running_jobs)}
            for fut in concurrent.futures.as_completed(fut_to_idx):
                idx = fut_to_idx[fut]
                rows_map[idx] = fut.result()
                progress.advance(ptask)

    port_cache.save()
    rows = sorted(
        (rows_map[i] for i in range(len(running_jobs))),
        key=lambda r: r["create_time"], reverse=True,
    )

    # 为 JSON/显示补充格式化字段
    quota_map = build_quota_annotations(api, rows)
    for r in rows:
        r["create_time_str"] = ts_to_str(r["create_time"]) if r["create_time"] else "--"
        r["duration_str"]    = ms_to_hms(r["duration_ms"])  if r["duration_ms"]  else "--"
        r.update(quota_map.get(r.get("job_id"), {}))

    if getattr(args, "json", False):
        _json_out({
            "minutes": args.minutes,
            "step": args.step,
            "jobs": rows,
        })
        return

    def _gpu_color(util, vram_used_mb, vram_total_mb):
        """Green = idle (util==0 AND vram<3%); Red = heavy; Yellow = in-use."""
        vram_pct = vram_used_mb / vram_total_mb if vram_total_mb else 0
        if util == 0 and vram_pct < 0.03:
            return "green"
        if util >= 0.8 or vram_pct >= 0.8:
            return "red"
        return "yellow"

    def _fmt_gpu_cell(r):
        devs = r.get("gpu_devices", [])
        if devs:
            parts = []
            for d in devs:
                color = _gpu_color(d.get("util", 0) or 0,
                                   d.get("vram_used_mb", 0) or 0,
                                   d.get("vram_total_mb", 1) or 1)
                parts.append(f"[{color}]GPU{d['index']} {(d.get('util') or 0)*100:.0f}%[/{color}]")
            return "\n".join(parts)
        return _fmt_usage_value("gpu_util", r["gpu"])

    def _fmt_vram_cell(r):
        devs = r.get("gpu_devices", [])
        if devs:
            parts = []
            for d in devs:
                color = _gpu_color(d.get("util", 0) or 0,
                                   d.get("vram_used_mb", 0) or 0,
                                   d.get("vram_total_mb", 1) or 1)
                parts.append(
                    f"[{color}]GPU{d['index']} "
                    f"{d.get('vram_used_mb') or 0:.0f}/{d.get('vram_total_mb') or 0:.0f}MB"
                    f"[/{color}]"
                )
            return "\n".join(parts)
        return _fmt_usage_value("gpu_mem_used_megabytes", r["gpu_mem"])

    # 多作业表格：按 filter_set 决定显示哪些列
    col_defs = [
        ("cpu",  "CPU",  lambda r: _fmt_usage_value("cpu_util", r["cpu"])),
        ("mem",  "\u5185\u5b58", lambda r: _fmt_usage_value("memory_used_megabytes", r["mem"])),
        ("gpu",  "GPU",  _fmt_gpu_cell),
        ("vram", "\u663e\u5b58", _fmt_vram_cell),
    ]
    active_cols = [(label, fmt) for key, label, fmt in col_defs if key in filter_set]

    t = Table(title="Running \u4f5c\u4e1a\u6700\u8fd1 usage", header_style="bold cyan", show_lines=True)
    t.add_column("\u540d\u79f0", style="green")
    t.add_column("JOB_ID", style="dim")
    t.add_column("SSH\u7aef\u53e3", style="cyan", no_wrap=True)
    t.add_column("\u521b\u5efa\u65f6\u95f4", style="dim", no_wrap=True)
    t.add_column("\u8fd0\u884c\u65f6\u957f", style="dim", no_wrap=True)
    for label, _ in active_cols:
        t.add_column(label)
    t.add_column("\u91c7\u96c6\u65f6\u95f4", style="dim", no_wrap=True)
    for row in rows:
        t.add_row(
            row["name"], row["job_id"], row.get("ssh_port", "\u2014"),
            row.get("create_time_str", "--"), row.get("duration_str", "--"),
            *[fmt(row) for _, fmt in active_cols],
            row.get("collected_at", ""),
        )
    console.print(t)
    if use_probe:
        cprint("[dim]\u4ec5\u663e\u793a Running \u4f5c\u4e1a\u6700\u8fd1 usage\uff1b[probe] \u5b9e\u65f6\u5355\u70b9\u91c7\u6837[/dim]")
    else:
        cprint(f"[dim]\u4ec5\u663e\u793a Running \u4f5c\u4e1a\u6700\u8fd1 usage\uff1b\u65f6\u95f4\u8303\u56f4: \u6700\u8fd1 {args.minutes} \u5206\u949f\uff0cstep={args.step}s[/dim]")
