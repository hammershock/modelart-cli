import sys
from macli.constants import STATUS_COLOR, console
from macli.log import cprint, dprint
from macli.helpers import (_json_out, job_to_dict, ms_to_hms, ts_to_str,
                           ssh_ports_list, ssh_ports_summary,
                           _resolve_jobs_ssh_map, _apply_job_filters,
                           _fetch_all_jobs)
from macli.session import _sess_or_exit, API
from rich.table import Table


def cmd_list_jobs(args):
    sess = _sess_or_exit()
    api  = API(sess)

    # 所有过滤在本地完成，分页拉取（每页最多 50，API 硬限制）
    need_filter = bool(args.recent or args.running or args.failed
                       or args.terminated or args.pending or args.gpu_count
                       or args.name or args.status)
    if need_filter:
        jobs = _fetch_all_jobs(api)
        total = len(jobs)
    else:
        PAGE = 50
        jobs     = []
        total    = 0
        page_num = 0
        while True:
            data  = api.list_jobs(limit=PAGE, offset=page_num)
            total = data.get("total", 0)
            page  = data.get("items", [])
            jobs += page
            page_num += 1
            if len(jobs) >= args.limit or not page or len(jobs) >= total:
                break

    jobs = _apply_job_filters(jobs, args)

    # ── count 模式：只输出数量 ──
    if getattr(args, "action", None) == "count":
        if getattr(args, "json", False):
            _json_out({"count": len(jobs)})
        else:
            print(len(jobs))
        return

    # ── SSH 端口解析（PortCache）─────────────────────────────
    ssh_map = _resolve_jobs_ssh_map(api, jobs, refresh=getattr(args, "refresh", False))

    if getattr(args, "json", False):
        out = []
        for j in jobs:
            job_id = j.get("metadata", {}).get("id", "")
            out.append(job_to_dict(j, ssh_override=ssh_map.get(job_id, [])))
        _json_out(out)
        return

    if not jobs:
        cprint("[yellow]没有找到符合条件的训练作业[/yellow]"); return

    t = Table(title=f"训练作业（总计 {total} 个，过滤后显示 {len(jobs)} 个）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#",        width=3)
    t.add_column("名称",     style="green", no_wrap=True, max_width=20)
    t.add_column("ID",       style="dim",   no_wrap=True, width=40)
    t.add_column("状态",     no_wrap=True,  width=13)
    t.add_column("时长",     width=10)
    t.add_column("卡数",     width=4)
    t.add_column("SSH端口",  width=12)
    t.add_column("创建时间", width=17)
    t.add_column("创建者",   width=10)

    for i, j in enumerate(jobs, 1):
        meta    = j.get("metadata", {})
        st      = j.get("status",   {})
        spec    = j.get("spec",     {})
        phase   = st.get("phase", "?")
        color   = STATUS_COLOR.get(phase, "white")
        gpu_num = spec.get("resource", {}).get("pool_info", {}).get("accelerator_num", "?")
        job_id  = meta.get("id", "")
        t.add_row(
            str(i), meta.get("name", ""), job_id,
            f"[{color}]{phase}[/{color}]",
            ms_to_hms(st.get("duration")), f"{gpu_num}卡",
            ssh_ports_summary(ssh_map.get(job_id, [])),
            ts_to_str(meta.get("create_time")), meta.get("user_name", ""),
        )
    console.print(t)
    if getattr(args, "refresh", False):
        cprint("[dim]已清空缓存并重新拉取所有端口信息（--refresh）[/dim]")
    else:
        cprint("[dim]端口缓存：Running 作业端口已自动缓存，--refresh 可强制重新拉取[/dim]")


def cmd_query(args):
    """按条件筛选作业，输出 ID（可管道给其他命令）。"""
    sess = _sess_or_exit()
    api  = API(sess)

    jobs = _apply_job_filters(_fetch_all_jobs(api), args)

    if getattr(args, "json", False):
        _json_out([{
            "id":     j.get("metadata", {}).get("id", ""),
            "name":   j.get("metadata", {}).get("name", ""),
            "status": j.get("status", {}).get("phase", ""),
        } for j in jobs])
        return

    # 管道模式：只输出 ID，每行一个，干净地传给下游命令
    if not sys.stdout.isatty():
        for j in jobs:
            print(j.get("metadata", {}).get("id", ""))
        return

    # 终端模式：显示预览表格（不解析 SSH 端口，速度快）
    if not jobs:
        cprint("[yellow]没有找到符合条件的训练作业[/yellow]")
        return

    t = Table(title=f"查询结果（共 {len(jobs)} 个作业）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#",        width=3)
    t.add_column("名称",     style="green", no_wrap=True, max_width=24)
    t.add_column("ID",       style="dim",   no_wrap=True, width=40)
    t.add_column("状态",     no_wrap=True,  width=13)
    t.add_column("时长",     width=10)
    t.add_column("卡数",     width=4)
    t.add_column("创建时间", width=17)

    for i, j in enumerate(jobs, 1):
        meta    = j.get("metadata", {})
        st      = j.get("status",   {})
        spec    = j.get("spec",     {})
        phase   = st.get("phase", "?")
        color   = STATUS_COLOR.get(phase, "white")
        gpu_num = spec.get("resource", {}).get("pool_info", {}).get("accelerator_num", "?")
        t.add_row(
            str(i), meta.get("name", ""), meta.get("id", ""),
            f"[{color}]{phase}[/{color}]",
            ms_to_hms(st.get("duration")), f"{gpu_num}卡",
            ts_to_str(meta.get("create_time")),
        )
    console.print(t)
    cprint("[dim]提示：将此命令管道给其他操作，例如：macli query [filters] | macli stop --yes[/dim]")


def cmd_ports(args):
    """列出当前 Running 作业的 SSH 端口信息。"""
    sess = _sess_or_exit()
    api  = API(sess)

    jobs = [j for j in _fetch_all_jobs(api)
            if j.get("status", {}).get("phase", "") == "Running"]
    ssh_map = _resolve_jobs_ssh_map(api, jobs, refresh=getattr(args, "refresh", False))

    if getattr(args, "json", False):
        out = []
        for j in jobs:
            meta    = j.get("metadata", {})
            res     = j.get("spec", {}).get("resource", {})
            job_id  = meta.get("id", "")
            ssh     = ssh_map.get(job_id, [])
            out.append({
                "id":          job_id,
                "name":        meta.get("name", ""),
                "status":      "Running",
                "create_time": meta.get("create_time"),
                "gpu_count":   res.get("pool_info", {}).get("accelerator_num") or 1,
                "ports":       ssh_ports_list(ssh),
                "ssh":         ssh,
            })
        _json_out(out)
        return

    if not jobs:
        cprint("[yellow]当前没有 Running 状态的训练作业[/yellow]")
        return

    t = Table(title=f"Running 作业 SSH 端口（共 {len(jobs)} 个）",
              header_style="bold cyan", show_lines=False)
    t.add_column("#", width=3)
    t.add_column("名称", style="green", no_wrap=True, max_width=24)
    t.add_column("ID", style="dim", no_wrap=True, width=40)
    t.add_column("SSH端口", width=18)

    for i, j in enumerate(jobs, 1):
        meta = j.get("metadata", {})
        job_id = meta.get("id", "")
        t.add_row(
            str(i),
            meta.get("name", ""),
            job_id,
            ssh_ports_summary(ssh_map.get(job_id, [])),
        )
    console.print(t)
    if getattr(args, "refresh", False):
        cprint("[dim]已清空缓存并重新拉取所有 Running 作业的端口信息（--refresh）[/dim]")
    else:
        cprint("[dim]端口缓存：Running 作业端口已自动缓存，--refresh 可强制重新拉取[/dim]")
