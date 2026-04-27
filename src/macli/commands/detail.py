import sys, time
from macli.constants import STATUS_COLOR, console
from macli.log import cprint, dprint
from macli.helpers import _json_out, job_to_dict, ms_to_hms, ts_to_str, _fmt_flavor, _fmt_actual
from macli.session import _sess_or_exit, API
from rich.table import Table
from rich.panel import Panel


def cmd_detail(args):
    sess = _sess_or_exit()
    api  = API(sess)

    if getattr(args, "src_name", None):
        data = api.list_jobs(limit=50)
        matched = [j for j in data.get("items", [])
                   if j.get("metadata", {}).get("name", "") == args.src_name]
        if not matched:
            cprint(f"[red]未找到名称为 '{args.src_name}' 的作业[/red]")
            sys.exit(1)
        if len(matched) > 1:
            cprint(f"[yellow]找到 {len(matched)} 个同名作业，使用最新的一个[/yellow]")
        job = matched[0]
        args.job_id = job.get("metadata", {}).get("id", "")
        # 获取完整详情（列表返回的字段可能不完整）
        job = api.get_job(args.job_id)
        if not job: sys.exit(1)
    else:
        job = api.get_job(args.job_id)
        if not job: sys.exit(1)

    if getattr(args, "json", False):
        _json_out(job_to_dict(job))
        return

    meta  = job.get("metadata", {})
    st    = job.get("status",   {})
    spec  = job.get("spec",     {})
    phase = st.get("phase", "?")
    color = STATUS_COLOR.get(phase, "white")

    console.print(Panel(
        f"[bold]名称:[/bold]   {meta.get('name','')}\n"
        f"[bold]状态:[/bold]   [{color}]{phase}[/{color}]\n"
        f"[bold]时长:[/bold]   {ms_to_hms(st.get('duration'))}\n"
        f"[bold]规格(目标):[/bold] {_fmt_flavor(spec.get('resource',{}))}\n"
        f"[bold]规格(实际):[/bold] {_fmt_actual(spec.get('resource',{}))}\n"
        f"[bold]镜像:[/bold]   {job.get('algorithm',{}).get('engine',{}).get('image_url','')}\n"
        f"[bold]创建者:[/bold] {meta.get('user_name','')}   "
        f"{ts_to_str(meta.get('create_time'))}",
        title=f"作业详情  {args.job_id}", border_style="cyan"))

    ssh_list = api.get_ssh(job)
    if ssh_list:
        cprint("\n[bold green]🔑 SSH 连接[/bold green]")
        for s in ssh_list:
            url = s["url"]
            port = s.get("port")
            port_show = port if port is not None else "—"
            cprint(f"  {s['task']} [dim](port: {port_show})[/dim]: {url}")
            if url.startswith("ssh://"):
                inner = url[6:]
                user, _, hp = inner.partition("@")
                host, _, port = hp.partition(":")
                cprint(f"  [bold cyan]ssh -p {port} -i ~/.ssh/KeyPair-liusonghua.pem {user}@{host}[/bold cyan]")
    else:
        cprint("\n[yellow]该作业暂无 SSH 信息（未运行或不是调试模式）[/yellow]")


def cmd_events(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job = api.get_job(args.job_id)
    if not job:
        sys.exit(1)
    create_time = (job.get("metadata", {}) or {}).get("create_time")
    end_time = int(time.time() * 1000)

    data = api.get_job_events(
        args.job_id,
        limit=args.limit,
        offset=args.offset,
        start_time=create_time,
        end_time=end_time,
        order="desc",
        pattern="",
        level="",
    )
    if not data:
        sys.exit(1)

    if getattr(args, "json", False):
        _json_out(data)
        return

    events = data.get("events", [])
    total = data.get("total", len(events))
    limit = data.get("limit", args.limit)
    offset = data.get("offset", args.offset)
    start_time = data.get("start_time", "")
    end_time = data.get("end_time", "")

    title = f"作业事件  {args.job_id}"
    if start_time or end_time:
        title += f"\n[dim]{start_time} ~ {end_time}[/dim]"
    t = Table(title=title, header_style="bold cyan", show_lines=False)
    t.add_column("#", width=4)
    t.add_column("时间", style="green", width=25, no_wrap=True)
    t.add_column("级别", width=10, no_wrap=True)
    t.add_column("来源", width=10, no_wrap=True)
    t.add_column("消息", overflow="fold")

    level_color = {
        "Info": "blue",
        "Warning": "yellow",
        "Error": "red",
        "Fatal": "bold red",
    }
    for i, ev in enumerate(events, start=offset + 1):
        level = ev.get("level", "")
        color = level_color.get(level, "white")
        t.add_row(
            str(i),
            ev.get("time", ""),
            f"[{color}]{level}[/{color}]" if level else "",
            ev.get("source", ""),
            ev.get("message", ""),
        )
    console.print(t)
    cprint(f"[dim]显示 {len(events)} / {total} 条事件（limit={limit}, offset={offset}）[/dim]")
