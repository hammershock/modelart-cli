"""辅助函数：解析、格式化、缓存、过滤"""
import os, sys, json, re, time, threading
from pathlib import Path
from datetime import datetime

from macli.constants import _CST
from macli.config import load_session, save_session, load_identityfiles
from macli.log import cprint, dprint


def _parse_ssh_url(url: str):
    """从 ssh://user@host:port 中提取 (user, host, port)，失败返回 (None, None, None)。"""
    if not url:
        return None, None, None
    m = re.match(r"^ssh://([^@]+)@([^:]+):(\d+)$", url.strip())
    if m:
        return m.group(1), m.group(2), int(m.group(3))
    return None, None, None


def resolve_identityfile(name_or_path: str) -> str:
    """将名称或路径解析为实际文件路径。
    - 若含路径分隔符或以 . 开头，视为路径直接使用
    - 否则在已保存的密钥列表中按名称查找
    - 找不到则原样返回（交由 SSH 自行报错）
    """
    if not name_or_path:
        return name_or_path
    if os.sep in name_or_path or name_or_path.startswith(".") or name_or_path.startswith("~"):
        return str(Path(name_or_path).expanduser())
    files, _ = load_identityfiles()
    if name_or_path in files:
        return str(Path(files[name_or_path]).expanduser())
    # 可能就是文件名（相对路径），直接返回
    return name_or_path


class PortCache:
    """
    Running 状态 job 的 SSH 端口缓存（线程安全写入）。

    规则：
    - 只有 Running 状态的 job 才可能持有 SSH 端口
    - 端口在 job 运行期间固定不变，结束时失效
    - job 一旦不再 Running，立即从缓存中驱逐
    - 持久化到 session.json["ssh_port_cache"]
    """
    _SESSION_KEY = "ssh_port_cache"

    def __init__(self):
        self._data: dict = {}   # {job_id: [{task, url, port}, ...]}
        self._dirty = False
        self._lock = threading.Lock()

    def load(self) -> "PortCache":
        self._data = load_session().get(self._SESSION_KEY, {})
        self._dirty = False
        return self

    def save(self):
        if not self._dirty:
            return
        with self._lock:
            d = load_session()
            d[self._SESSION_KEY] = self._data
            save_session(d)
            self._dirty = False

    def get(self, job_id: str):
        """缓存命中返回 ssh entries 列表；未命中返回 None。"""
        return self._data.get(job_id)

    def put(self, job_id: str, entries: list):
        """写入非空 entries（线程安全）。"""
        if not entries:
            return
        with self._lock:
            self._data[job_id] = entries
            self._dirty = True

    def evict(self, job_id: str):
        with self._lock:
            if job_id in self._data:
                del self._data[job_id]
                self._dirty = True

    def evict_non_running(self, running_ids: set) -> list:
        """清除不在 running_ids 中的所有缓存条目，返回被清除的 ID 列表。"""
        with self._lock:
            stale = [k for k in list(self._data) if k not in running_ids]
            for k in stale:
                del self._data[k]
            if stale:
                self._dirty = True
        return stale


def resolve_ssh(api: "API", job_id: str, phase: str,
                cache: "PortCache", detail_hint: dict = None) -> list:
    """
    获取 job 的 SSH entries（带缓存）。

    - 非 Running 状态：驱逐缓存，返回 []
    - Running + 缓存命中：直接返回缓存
    - Running + 缓存未命中：用 detail_hint 或拉取 detail，有端口则写缓存
    """
    if phase != "Running":
        cache.evict(job_id)
        return []
    cached = cache.get(job_id)
    if cached is not None:
        return cached
    detail = detail_hint or api.get_job(job_id)
    if not detail:
        return []
    entries = enrich_ssh_entries(
        detail.get("endpoints", {}).get("ssh", {}).get("task_urls", []),
        detail.get("status", {}).get("task_ips", []),
    )
    cache.put(job_id, entries)
    return entries


def parse_recent(s: str):
    """解析 --recent 参数，返回毫秒数。格式: 4d / 5h / 3m(月) / 1y"""
    if not s:
        return None
    s = s.strip().lower()
    import re as _re
    m = _re.match(r'^(\d+)(h|d|m|y)$', s)
    if not m:
        raise ValueError(f"无法解析时间格式: {s!r}，支持: 4d / 5h / 3m / 1y")
    n, unit = int(m.group(1)), m.group(2)
    seconds = {"h": 3600, "d": 86400, "m": 86400 * 30, "y": 86400 * 365}[unit]
    return n * seconds * 1000  # 转毫秒


def job_to_dict(j: dict, ssh_override: list = None, quota: dict = None) -> dict:
    """将 API 返回的 job 对象提炼为简洁的可序列化字典。
    ssh_override: 若传入，用此值替代 j 中的 endpoints.ssh（用于缓存注入场景）。
    """
    meta = j.get("metadata", {})
    st   = j.get("status",   {})
    spec = j.get("spec",     {})
    res  = spec.get("resource", {})
    if ssh_override is not None:
        ssh = ssh_override
    else:
        ssh = enrich_ssh_entries(
            j.get("endpoints", {}).get("ssh", {}).get("task_urls", []),
            st.get("task_ips", []),
        )
    out = {
        "id":          meta.get("id", ""),
        "name":        meta.get("name", ""),
        "status":      st.get("phase", ""),
        "duration_ms": st.get("duration"),
        "duration":    ms_to_hms(st.get("duration")),
        "gpu_count":   res.get("pool_info", {}).get("accelerator_num"),
        "flavor_id":   res.get("flavor_id", ""),
        "image":       j.get("algorithm", {}).get("engine", {}).get("image_url", ""),
        "create_time": meta.get("create_time"),
        "create_time_str": ts_to_str(meta.get("create_time")),
        "user_name":   meta.get("user_name", ""),
        "description": meta.get("description", ""),
        "ssh":         ssh,
    }
    if quota:
        out.update(quota)
    return out


def _json_out(data):
    """输出 JSON 到 stdout"""
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _fmt_flavor(resource: dict) -> str:
    """目标规格：来自 pool_info"""
    p   = resource.get("pool_info", {})
    gpu = p.get("accelerator_num", "?")
    acc = p.get("accelerator_type", "?")   # nvidia-a100-nv80
    cpu = p.get("core_num", "?")
    mem = p.get("mem_size", "?")
    return f"{gpu}*{acc} | {cpu}vCPUs | {mem}GiB"

def _fmt_actual(resource: dict) -> str:
    """实际分配：来自 main_container_allocated_resources"""
    a = resource.get("main_container_allocated_resources", {})
    if not a:
        return "(未分配)"
    gpu = a.get("accelerator_num", "?")
    acc = a.get("accelerator_type", "?")
    cpu = a.get("cpu_core_num", "?")
    mem = a.get("mem_size", "?")
    return f"{gpu}*{acc} | {cpu}vCPUs | {mem}GiB"

def ms_to_hms(ms):
    if not ms: return "--"
    try:
        h, rem = divmod(int(ms) // 1000, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    except: return str(ms)

def ts_to_str(ts):
    if not ts: return "--"
    try: return datetime.fromtimestamp(int(ts) / 1000, tz=_CST).strftime("%Y-%m-%d %H:%M")
    except: return str(ts)


def ssh_url_to_port(url: str):
    """从 ssh://user@host:port URL 中提取端口号；取不到则返回 None。"""
    if not url or not isinstance(url, str):
        return None
    m = re.match(r"^ssh://[^@]+@[^:]+:(\d+)$", url.strip())
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def enrich_ssh_entries(entries: list, task_ips: list = None) -> list:
    """为 SSH 条目补充 port / pod_ip / host_ip 字段。"""
    task_ip_map = {}
    for item in task_ips or []:
        task = item.get("task", "")
        if task:
            task_ip_map[task] = item
    fallback_ip = (task_ips or [{}])[0] if len(task_ips or []) == 1 else {}

    out = []
    for item in entries or []:
        url = item.get("url", "")
        task = item.get("task", "")
        ip_info = task_ip_map.get(task, fallback_ip)
        out.append({
            "task": task,
            "url": url,
            "port": ssh_url_to_port(url),
            "pod_ip": ip_info.get("ip", ""),
            "host_ip": ip_info.get("host_ip", ""),
        })
    return out


def ssh_ports_list(entries: list) -> list:
    """提取并去重 SSH 端口列表，返回 int 列表。"""
    ports = []
    for item in entries or []:
        port = item.get("port")
        if port is None:
            port = ssh_url_to_port(item.get("url", ""))
        if port is not None:
            ports.append(int(port))
    return list(dict.fromkeys(ports))



def ssh_ports_summary(entries: list) -> str:
    """用于表格展示 SSH 端口。多个端口去重后以逗号连接；缺失返回 —。"""
    ports = ssh_ports_list(entries)
    return ",".join(map(str, ports)) if ports else "—"


_QUOTA_ACTIVE_PHASES = {"Running", "Pending", "Waiting"}


def _to_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _quota_item_id(item: dict) -> str:
    return (
        item.get("job_id")
        or item.get("id")
        or item.get("metadata", {}).get("id", "")
    )


def _quota_item_name(item: dict) -> str:
    return item.get("name") or item.get("metadata", {}).get("name", "")


def _quota_item_phase(item: dict) -> str:
    status = item.get("status")
    if isinstance(status, dict):
        return status.get("phase", "")
    if isinstance(status, str):
        return status
    # usage --probe rows are produced only for running jobs.
    if item.get("job_id"):
        return "Running"
    return ""


def _quota_item_create_time(item: dict) -> int:
    return _to_int(
        item.get("create_time")
        or item.get("metadata", {}).get("create_time"),
        0,
    )


def _quota_item_pool_id(item: dict) -> str:
    if item.get("pool_id"):
        return item.get("pool_id")
    res = item.get("spec", {}).get("resource", {})
    return (
        res.get("pool_id")
        or res.get("pool_info", {}).get("pool_id", "")
    )


def _quota_item_gpu_count(item: dict) -> int:
    n = _to_int(item.get("gpu_count"))
    if n is not None and n > 0:
        return n
    devices = item.get("gpu_devices") or []
    if devices:
        return max(1, len(devices))
    res = item.get("spec", {}).get("resource", {})
    pool_info = res.get("pool_info", {})
    per_node = _to_int(
        pool_info.get("accelerator_num")
        or res.get("main_container_allocated_resources", {}).get("accelerator_num"),
        1,
    )
    node_count = _to_int(res.get("node_count"), 1)
    return max(1, per_node * node_count)


def _quota_base_labels(pool_id: str, guaranteed_gpu, max_gpu) -> list:
    labels = ["fifo"]
    if pool_id:
        labels.append(f"pool:{pool_id}")
    if guaranteed_gpu is not None or max_gpu is not None:
        labels.append(f"quota:{guaranteed_gpu if guaranteed_gpu is not None else '?'}/"
                      f"{max_gpu if max_gpu is not None else '?'}")
    return labels


def _quota_annotation(quota_class: str, preemptible: bool, labels: list,
                      reason: str, rank=None, used_before=None, used_after=None,
                      guaranteed_gpu=None, max_gpu=None) -> dict:
    return {
        "preemptible": preemptible,
        "quota_class": quota_class,
        "quota_labels": labels,
        "quota_rank": rank,
        "quota_used_before": used_before,
        "quota_used_after": used_after,
        "quota_guaranteed_gpu": guaranteed_gpu,
        "quota_max_gpu": max_gpu,
        "quota_reason": reason,
    }


def _pool_metrics_index(api: "API") -> dict:
    workspace_id = getattr(api.sess, "workspace_id", None)
    data = api.get_pool_runtime_metrics(workspace_id=workspace_id)
    idx = {}
    for item in data.get("items", []) if isinstance(data, dict) else []:
        name = item.get("metadata", {}).get("name", "")
        cap = item.get("table", {}).get("capacity", {})
        value = cap.get("value", {}) or {}
        max_value = cap.get("maxValue", {}) or {}
        guaranteed = _to_int(value.get("nvidia.com/gpu"))
        max_gpu = _to_int(max_value.get("nvidia.com/gpu"))
        if name and guaranteed is not None:
            idx[name] = {
                "guaranteed_gpu": guaranteed,
                "max_gpu": max_gpu,
                "source": "metrics",
            }
    return idx


def _resource_flavor_gpu_sizes(api: "API") -> dict:
    sizes = {}
    for item in api.list_resource_flavors():
        name = item.get("metadata", {}).get("name", "")
        spec = item.get("spec", {}) or {}
        gpu = spec.get("gpu") or spec.get("npu") or {}
        size = _to_int(gpu.get("size"))
        if name and size is not None:
            sizes[name] = size
    return sizes


def _pool_quota(api: "API", pool_id: str, metrics: dict,
                flavor_sizes: dict = None) -> dict:
    if pool_id in metrics:
        q = dict(metrics[pool_id])
        q["reason"] = "runtime metrics"
        return q

    pool = api.get_resource_pool(pool_id)
    resources = pool.get("spec", {}).get("resources", []) if pool else []
    if not resources:
        return {"reason": "resource pool quota unavailable"}

    if flavor_sizes is None:
        flavor_sizes = _resource_flavor_gpu_sizes(api)

    guaranteed = 0
    max_gpu = 0
    missing_flavors = []
    for res in resources:
        flavor = res.get("flavor", "")
        gpu_size = flavor_sizes.get(flavor)
        if gpu_size is None:
            missing_flavors.append(flavor or "(unknown)")
            continue
        guaranteed += (_to_int(res.get("count"), 0) or 0) * gpu_size
        max_gpu += (_to_int(res.get("maxCount"), 0) or 0) * gpu_size

    if missing_flavors:
        return {
            "reason": "resource flavor GPU size unavailable: "
                      + ",".join(missing_flavors)
        }
    return {
        "guaranteed_gpu": guaranteed,
        "max_gpu": max_gpu,
        "source": "pool_spec",
        "reason": "resource pool spec",
    }


def build_quota_annotations(api: "API", items: list) -> dict:
    """按资源队列保障配额和 FIFO 顺序为作业/usage rows 生成保障标签。

    返回 {job_id: quota_metadata}。无法读取配额时不回退到固定卡数。
    """
    result = {}
    active_by_pool = {}

    for item in items or []:
        job_id = _quota_item_id(item)
        if not job_id:
            continue
        phase = _quota_item_phase(item)
        if phase not in _QUOTA_ACTIVE_PHASES:
            result[job_id] = _quota_annotation(
                "inactive", False, ["inactive"],
                f"phase {phase or 'unknown'} is not active",
            )
            continue
        pool_id = _quota_item_pool_id(item)
        if not pool_id:
            result[job_id] = _quota_annotation(
                "unknown", True, ["unknown", "preemptible"],
                "missing pool_id",
            )
            continue
        active_by_pool.setdefault(pool_id, []).append(item)

    if not active_by_pool:
        return result

    try:
        metrics = _pool_metrics_index(api)
    except Exception as e:
        dprint(f"[dim]资源池运行指标不可用: {e}[/dim]")
        metrics = {}

    flavor_sizes = None
    quota_cache = {}
    for pool_id, pool_items in active_by_pool.items():
        if pool_id not in quota_cache:
            try:
                quota_cache[pool_id] = _pool_quota(
                    api, pool_id, metrics, flavor_sizes
                )
                if quota_cache[pool_id].get("source") != "metrics" and flavor_sizes is None:
                    flavor_sizes = _resource_flavor_gpu_sizes(api)
                    quota_cache[pool_id] = _pool_quota(
                        api, pool_id, metrics, flavor_sizes
                    )
            except Exception as e:
                quota_cache[pool_id] = {"reason": str(e)}
        quota = quota_cache[pool_id]
        guaranteed_gpu = quota.get("guaranteed_gpu")
        max_gpu = quota.get("max_gpu")
        base_labels = _quota_base_labels(pool_id, guaranteed_gpu, max_gpu)

        if guaranteed_gpu is None:
            for item in pool_items:
                job_id = _quota_item_id(item)
                result[job_id] = _quota_annotation(
                    "unknown", True,
                    ["unknown", "preemptible"] + base_labels,
                    quota.get("reason", "resource pool quota unavailable"),
                    guaranteed_gpu=guaranteed_gpu,
                    max_gpu=max_gpu,
                )
            continue

        used = 0
        ordered = sorted(
            pool_items,
            key=lambda x: (_quota_item_create_time(x), _quota_item_id(x)),
        )
        for rank, item in enumerate(ordered, 1):
            job_id = _quota_item_id(item)
            need = _quota_item_gpu_count(item)
            before = used
            after = used + need
            used = after
            if after <= guaranteed_gpu:
                labels = ["guaranteed", "stable"] + base_labels
                reason = (f"FIFO rank {rank}: cumulative GPUs {after}/"
                          f"{guaranteed_gpu} within guaranteed quota")
                result[job_id] = _quota_annotation(
                    "guaranteed", False, labels, reason,
                    rank=rank, used_before=before, used_after=after,
                    guaranteed_gpu=guaranteed_gpu, max_gpu=max_gpu,
                )
            else:
                labels = ["elastic", "preemptible"] + base_labels
                if max_gpu is not None and after > max_gpu:
                    labels.append("over-max")
                reason = (f"FIFO rank {rank}: cumulative GPUs {after} "
                          f"exceeds guaranteed quota {guaranteed_gpu}")
                result[job_id] = _quota_annotation(
                    "elastic", True, labels, reason,
                    rank=rank, used_before=before, used_after=after,
                    guaranteed_gpu=guaranteed_gpu, max_gpu=max_gpu,
                )
    return result


def _resolve_jobs_ssh_map(api: "API", jobs: list, refresh: bool = False) -> dict:
    """为一组 jobs 解析 SSH entries，并复用 Running 作业的端口缓存。"""
    port_cache = PortCache() if refresh else PortCache().load()
    if refresh:
        dprint("[dim]--refresh: 已清空端口缓存，强制重新拉取[/dim]")

    running_ids = {j.get("metadata", {}).get("id", "") for j in jobs
                   if j.get("status", {}).get("phase") == "Running"
                   and j.get("metadata", {}).get("id")}
    stale = port_cache.evict_non_running(running_ids)
    if stale:
        dprint(f"[dim]清理 {len(stale)} 条非 Running 端口缓存[/dim]")

    ssh_map: dict = {}
    hit_count = fetched_count = 0
    for j in jobs:
        job_id = j.get("metadata", {}).get("id", "")
        phase  = j.get("status", {}).get("phase", "")
        if not job_id:
            continue
        before = port_cache.get(job_id)
        ssh_map[job_id] = resolve_ssh(api, job_id, phase, port_cache)
        if before is not None:
            hit_count += 1
        elif phase == "Running":
            fetched_count += 1
    port_cache.save()
    dprint(f"[dim]端口缓存：命中 {hit_count} 条，新拉取 {fetched_count} 条[/dim]")
    return ssh_map



_STATUS_ALIAS: dict = {
    "running":    {"Running"},
    "failed":     {"Failed"},
    "terminated": {"Stopped", "Terminated"},
    "pending":    {"Pending", "Waiting"},
}


def _apply_job_filters(jobs: list, args) -> list:
    """根据 args 中的过滤条件对作业列表进行本地过滤，返回过滤后的列表。"""
    if getattr(args, "recent", None):
        try:
            delta_ms = parse_recent(args.recent)
        except ValueError as e:
            cprint(f"[red]{e}[/red]"); sys.exit(1)
        cutoff = int(time.time() * 1000) - delta_ms
        jobs = [j for j in jobs
                if (j.get("metadata", {}).get("create_time") or 0) >= cutoff]

    status_filter: set = set()
    if getattr(args, "running",    False): status_filter |= _STATUS_ALIAS["running"]
    if getattr(args, "failed",     False): status_filter |= _STATUS_ALIAS["failed"]
    if getattr(args, "terminated", False): status_filter |= _STATUS_ALIAS["terminated"]
    if getattr(args, "pending",    False): status_filter |= _STATUS_ALIAS["pending"]
    for s in (getattr(args, "status", None) or []):
        status_filter |= _STATUS_ALIAS.get(s.lower(), {s})
    if status_filter:
        jobs = [j for j in jobs
                if j.get("status", {}).get("phase", "") in status_filter]

    if getattr(args, "gpu_count", None):
        allowed = set(args.gpu_count)
        jobs = [j for j in jobs
                if j.get("spec", {}).get("resource", {})
                    .get("pool_info", {}).get("accelerator_num") in allowed]

    if getattr(args, "name", None):
        jobs = [j for j in jobs
                if j.get("metadata", {}).get("name", "") == args.name]

    limit = getattr(args, "limit", None)
    if limit:
        jobs = jobs[:limit]
    dprint(f"[dim]_apply_job_filters: 过滤后剩余 {len(jobs)} 个作业[/dim]")
    return jobs


def _read_piped_ids() -> list:
    """从 stdin 读取作业 ID（每行一个）。若 stdin 是终端则返回空列表。"""
    if sys.stdin.isatty():
        return []
    return [ln.strip() for ln in sys.stdin if ln.strip()]


def _fetch_all_jobs(api: "API", max_items: int = 500) -> list:
    """分页拉取所有作业。
    API 的 offset 是页码（0-indexed），每页最多 50 条，limit 硬限 ≤50。
    """
    PAGE = 50
    jobs: list = []
    page_num = 0
    total = 0
    while True:
        data  = api.list_jobs(limit=PAGE, offset=page_num)
        total = data.get("total", 0)
        page  = data.get("items", [])
        jobs += page
        page_num += 1
        if not page or len(jobs) >= total or len(jobs) >= max_items:
            break
    dprint(f"[dim]_fetch_all_jobs: 共拉取 {len(jobs)} 个作业（total={total}）[/dim]")
    return jobs
