import sys, time
from datetime import datetime
from macli.constants import _CST, REGION_NAMES, STATUS_COLOR, console
from macli.config import load_session, save_session
from macli.log import cprint, dprint
from macli.helpers import _json_out
from macli.session import ConsoleSession, _sess_or_exit
from macli.auth import _me, _fetch_workspaces, _select_workspace
from rich.table import Table
from rich.panel import Panel


def cmd_workspace_list(args):
    sess = _sess_or_exit()
    workspaces = _fetch_workspaces(sess)
    if not workspaces:
        cprint("[yellow]未获取到工作空间[/yellow]"); return

    current = sess.workspace_id
    if getattr(args, "json", False):
        _json_out([{"id": ws["id"], "name": ws["name"],
                    "status": ws.get("status", ""),
                    "description": ws.get("description", ""),
                    "current": ws["id"] == current} for ws in workspaces])
        return

    t = Table(title="工作空间列表", header_style="bold cyan")
    t.add_column("",     width=2)
    t.add_column("名称", style="green")
    t.add_column("状态", width=8)
    t.add_column("描述")
    for ws in workspaces:
        marker = "[bold yellow]*[/bold yellow]" if ws["id"] == current else " "
        sc = "green" if ws.get("status") == "NORMAL" else "yellow"
        t.add_row(marker, ws["name"],
                  f"[{sc}]{ws.get('status','')}[/{sc}]",
                  ws.get("description") or "")
    console.print(t)
    cprint("[dim]* 当前选中[/dim]")

def cmd_workspace_select(args):
    sess = _sess_or_exit()
    workspaces = _fetch_workspaces(sess)
    if not workspaces:
        cprint("[yellow]未获取到工作空间[/yellow]"); return

    wsid = None
    if args.id:
        for ws in workspaces:
            if ws["id"] == args.id:
                wsid = ws["id"]; chosen_name = ws["name"]; break
        if not wsid:
            cprint(f"[red]未找到 ID={args.id}[/red]"); sys.exit(1)
    elif args.name:
        for ws in workspaces:
            if ws["name"] == args.name:
                wsid = ws["id"]; chosen_name = ws["name"]; break
        if not wsid:
            cprint(f"[red]未找到名称 '{args.name}'[/red]"); sys.exit(1)
    else:
        wsid = _select_workspace(sess)
        chosen_name = wsid

    sess.workspace_id = wsid
    d = load_session(); d["workspace_id"] = wsid; save_session(d)
    if args.id or args.name:
        cprint(f"[green]✓ 已切换到：{chosen_name}[/green]")


def cmd_region_list(args):
    """列出 cookie 中记录的 region，以及当前选中的"""
    d = load_session()
    if not d:
        cprint("[red]未登录[/red]"); return
    current = d.get("region", "")
    # 重新从 me 接口拿最新列表
    sess = ConsoleSession(); sess.restore()
    me = _me(sess.http, current, sess.cftk, sess.agency_id)
    regions = sorted(r for r in me.get("supportRegions", [current]) if r in REGION_NAMES or r == current)
    if getattr(args, "json", False):
        _json_out([{"id": r, "name_cn": REGION_NAMES.get(r, ""),
                    "current": r == current} for r in regions])
        return

    t = Table(title="可用区域", header_style="bold cyan")
    t.add_column("",       width=2)
    t.add_column("区域",   style="green", no_wrap=True)
    t.add_column("中文名", style="dim")
    for r in regions:
        marker = "[bold yellow]*[/bold yellow]" if r == current else " "
        t.add_row(marker, r, REGION_NAMES.get(r, ""))
    console.print(t)
    cprint("[dim]* 当前选中[/dim]")

def cmd_region_select(args):
    """切换区域，自动更新 project_id，并重新选择工作空间"""
    d = load_session()
    if not d:
        cprint("[red]未登录[/red]"); return

    sess = ConsoleSession(); sess.restore()
    me = _me(sess.http, sess.region, sess.cftk, sess.agency_id)
    regions = me.get("supportRegions", [])

    # 确定目标 region
    if args.name:
        if args.name not in regions:
            cprint(f"[red]'{args.name}' 不在可用区域列表中[/red]"); sys.exit(1)
        region = args.name
    else:
        if not regions:
            cprint("[red]无法获取区域列表[/red]"); sys.exit(1)
        regions = sorted(r for r in regions if r in REGION_NAMES)
        cprint("\n[bold]可用区域：[/bold]")
        for i, r in enumerate(regions, 1):
            label = f"{r}  [dim]{REGION_NAMES[r]}[/dim]"
            cprint(f"  [cyan]{i}.[/cyan] {label}")
        while True:
            choice = input(f"\n请选择区域 (1-{len(regions)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(regions):
                region = regions[int(choice) - 1]; break
            cprint("[red]输入无效，请重试[/red]")

    # 获取新 project_id
    dprint(f"[cyan]获取 {region} 的 project_id...[/cyan]")
    me2 = _me(sess.http, region, sess.cftk, sess.agency_id)
    project_id = me2.get("projectId", "")
    if not project_id:
        cprint(f"[red]无法获取 {region} 的 project_id[/red]"); sys.exit(1)
    dprint(f"[green]✓ region={region}  project_id={project_id}[/green]")

    # 更新 session
    sess.region = region
    sess.project_id = project_id
    sess._set_headers()
    me2_agency = me2.get("id") or me2.get("userId", "")
    d = load_session()
    d["region"]     = region
    d["project_id"] = project_id
    d["agency_id"]  = me2_agency
    d["workspace_id"] = ""
    save_session(d)

    # 自动选第一个工作空间
    dprint("[cyan]获取工作空间列表...[/cyan]")
    workspaces = _fetch_workspaces(sess)
    if workspaces:
        wsid = workspaces[0]["id"]
        wsname = workspaces[0]["name"]
        sess.workspace_id = wsid
        d = load_session(); d["workspace_id"] = wsid; save_session(d)
        cprint(f"[green]✓ 已切换到 {region}，工作空间: {wsname}[/green]")
        dprint("[dim]提示：使用 workspace select 切换工作空间[/dim]")
    else:
        cprint(f"[green]✓ 已切换到 {region}[/green]")
        cprint("[yellow]未获取到工作空间，使用 workspace select 手动设置[/yellow]")


def cmd_whoami(args):
    d = load_session()
    if not d: cprint("[red]未登录[/red]"); return
    ck = d.get("cookies", {})
    age = (time.time() - d.get("saved_at", 0)) / 3600

    if getattr(args, "json", False):
        _json_out({
            "user":         ck.get("masked_user", ""),
            "domain":       ck.get("masked_domain", ""),
            "region":       d.get("region", ""),
            "project_id":   d.get("project_id", ""),
            "agency_id":    d.get("agency_id", ""),
            "workspace_id": d.get("workspace_id", ""),
            "saved_at":     d.get("saved_at"),
            "session_age_hours": round(age, 1),
        })
        return

    console.print(Panel(
        f"[bold]用户:[/bold]         {ck.get('masked_user','?')}\n"
        f"[bold]租户:[/bold]         {ck.get('masked_domain','?')}\n"
        f"[bold]Region:[/bold]       {d.get('region','?')}\n"
        f"[bold]Project ID:[/bold]   {d.get('project_id','?')}\n"
        f"[bold]User ID:[/bold]      {d.get('agency_id','?')}\n"
        f"[bold]Workspace ID:[/bold] {d.get('workspace_id','?')}\n"
        f"[bold]保存于:[/bold]       "
        f"{datetime.fromtimestamp(d.get('saved_at',0), tz=_CST).strftime('%Y-%m-%d %H:%M')}"
        f"  ({age:.1f}h 前)",
        title="Session 状态", border_style="green"))
