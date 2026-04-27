import sys, re, time
from pathlib import Path

from macli.constants import STATUS_COLOR, console, Confirm
from macli.log import cprint, dprint
from macli.helpers import _json_out, job_to_dict, _read_piped_ids
from macli.session import _sess_or_exit, API
from rich.table import Table
from rich.panel import Panel


def cmd_copy(args):
    sess = _sess_or_exit()
    api  = API(sess)

    # 支持按 JOB_ID 或 --src-name 指定源作业
    if getattr(args, "src_name", None):
        # 按名称查找：从作业列表里匹配
        data = api.list_jobs(limit=50)
        items = data.get("items", [])
        matched = [j for j in items
                   if j.get("metadata", {}).get("name", "") == args.src_name]
        if not matched:
            cprint(f"[red]未找到名称为 '{args.src_name}' 的作业[/red]")
            sys.exit(1)
        if len(matched) > 1:
            cprint(f"[yellow]找到 {len(matched)} 个同名作业，使用最新的一个[/yellow]")
        job = matched[0]
        args.job_id = job.get("metadata", {}).get("id", "")
    else:
        job = api.get_job(args.job_id)
        if not job: sys.exit(1)

    meta      = job.get("metadata", {})
    spec      = job.get("spec", {})
    algo      = job.get("algorithm", {})
    cur_flavor= spec.get("resource", {}).get("flavor_id", "")
    cur_cmd   = algo.get("command", "")
    cur_name  = meta.get("name", "")
    cur_desc  = meta.get("description", "")

    # 计算新名称（用于预览）
    if args.name:
        new_name = args.name
    else:
        clean    = re.sub(r'-copy-\d+$', '', cur_name)
        new_name = f"{clean}-copy-{int(time.time()) % 100000}"

    # --json 模式：跳过所有交互和表格，直接提交
    if not getattr(args, "json", False):
        full_cmd    = args.command or cur_cmd
        cmd_preview = full_cmd.replace("\n", " ; ")[:60]
        cmd_suffix  = "..." if len(full_cmd) > 60 else ""
        desc_show   = args.description if args.description is not None else (cur_desc or "(无)")
        gpu_show    = str(args.gpu_count) if args.gpu_count else "(不变)"
        console.print(Panel(
            f"[bold]原作业:[/bold]       {cur_name}\n"
            f"[bold]新名称:[/bold]       {new_name}\n"
            f"[bold]描述:[/bold]         {desc_show}\n"
            f"[bold]当前规格:[/bold]     {cur_flavor}\n"
            f"[bold]新GPU卡数:[/bold]    {gpu_show}\n"
            f"[bold]启动命令:[/bold]     {cmd_preview}{cmd_suffix}",
            title="复制训练作业", border_style="yellow"))

        if not args.yes:
            ok = Confirm.ask("确认创建？")
            if not ok: cprint("[yellow]已取消[/yellow]"); return

    # --command-file 优先于 --command
    if args.command_file:
        try:
            args.command = Path(args.command_file).read_text(encoding="utf-8").strip()
            if not getattr(args, "json", False):
                dprint(f"[green]✓ 从 {args.command_file} 读取启动命令[/green]")
        except Exception as e:
            cprint(f"[red]读取命令文件失败: {e}[/red]"); sys.exit(1)

    if not getattr(args, "json", False):
        dprint("[cyan]提交中...[/cyan]")
    result = api.copy_job(
        args.job_id,
        new_gpu_count = args.gpu_count,
        new_name      = args.name,
        description   = args.description,
        command       = args.command,
    )

    if result:
        if getattr(args, "json", False):
            _json_out(job_to_dict(result))
        else:
            nm = result.get("metadata", {})
            cprint(f"[green]✓ 创建成功！[/green]")
            cprint(f"  名称: {nm.get('name','')}")
            cprint(f"  ID:   {nm.get('id','')}")
            cprint(f"  状态: {result.get('status',{}).get('phase','等待中')}")
    else:
        cprint("[red]创建失败[/red]")
        sys.exit(1)


def cmd_stop(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job_ids = list(getattr(args, "job_ids", None) or []) or _read_piped_ids()
    if not job_ids:
        cprint("[red]请提供 JOB_ID 或通过管道传入（macli query ... | macli stop）[/red]")
        sys.exit(1)

    # 收集作业信息（去除不存在的）
    jobs_info = []
    for job_id in job_ids:
        job = api.get_job(job_id)
        if not job:
            cprint(f"[red]未找到作业 {job_id}，跳过[/red]")
            continue
        jobs_info.append((job_id,
                          job.get("metadata", {}).get("name", ""),
                          job.get("status", {}).get("phase", "")))

    if not jobs_info:
        cprint("[yellow]没有可终止的作业[/yellow]"); return

    # 预览
    t = Table(title=f"即将终止 {len(jobs_info)} 个作业", header_style="bold red")
    t.add_column("名称", style="green")
    t.add_column("ID",   style="dim")
    t.add_column("状态")
    for job_id, name, phase in jobs_info:
        color = STATUS_COLOR.get(phase, "white")
        t.add_row(name, job_id, f"[{color}]{phase}[/{color}]")
    console.print(t)

    if not args.yes:
        ok = Confirm.ask(f"[red]确认终止以上 {len(jobs_info)} 个作业？[/red]")
        if not ok: cprint("[yellow]已取消[/yellow]"); return

    ok_count = 0
    for job_id, name, _ in jobs_info:
        if api.stop_job(job_id):
            cprint(f"[green]✓ 已发送终止指令: {name}[/green]")
            ok_count += 1
        else:
            cprint(f"[red]✗ 终止失败: {name}[/red]")
    if len(jobs_info) > 1:
        cprint(f"[green]完成：{ok_count}/{len(jobs_info)} 个作业已终止[/green]")


def cmd_delete(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job_ids = list(getattr(args, "job_ids", None) or []) or _read_piped_ids()
    if not job_ids:
        cprint("[red]请提供 JOB_ID 或通过管道传入（macli query ... | macli delete）[/red]")
        sys.exit(1)

    # 收集所有作业信息
    jobs_info = []
    for job_id in job_ids:
        job = api.get_job(job_id)
        if not job:
            cprint(f"[red]未找到作业 {job_id}，跳过[/red]")
            continue
        name  = job.get("metadata", {}).get("name", "")
        phase = job.get("status", {}).get("phase", "")
        if phase == "Running" and not args.force:
            cprint(f"[red]{name} 正在运行中，跳过（使用 -f 强制删除）[/red]")
            continue
        jobs_info.append((job_id, name, phase))

    if not jobs_info:
        cprint("[yellow]没有可删除的作业[/yellow]"); return

    # 预览列表
    t = Table(title=f"即将删除 {len(jobs_info)} 个作业", header_style="bold red")
    t.add_column("名称", style="green")
    t.add_column("ID",   style="dim")
    t.add_column("状态")
    for job_id, name, phase in jobs_info:
        color = "red" if phase == "Running" else STATUS_COLOR.get(phase, "white")
        t.add_row(name, job_id, f"[{color}]{phase}[/{color}]")
    console.print(t)
    if any(ph == "Running" for _, _, ph in jobs_info):
        cprint("[bold red]⚠ 包含运行中的作业，将被强制删除！[/bold red]")
    cprint("[red]此操作无法恢复！[/red]")
    if not args.yes:
        prompt = f"确认删除以上 {len(jobs_info)} 个作业？"
        ok = Confirm.ask(f"[red]{prompt}[/red]")
        if not ok: cprint("[yellow]已取消[/yellow]"); return

    # 逐个删除
    ok_count = 0
    for job_id, name, phase in jobs_info:
        if api.delete_job(job_id):
            cprint(f"[green]✓ 已删除: {name}[/green]")
            ok_count += 1
        else:
            cprint(f"[red]✗ 删除失败: {name}[/red]")

    cprint(f"[green]完成：{ok_count}/{len(jobs_info)} 个作业已删除[/green]")
