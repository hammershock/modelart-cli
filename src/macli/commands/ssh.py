import os, sys

from macli.log import cprint, dprint
from macli.helpers import PortCache, resolve_ssh
from macli.session import _sess_or_exit, API
from macli.commands.exec_ import _build_ssh_cmd


def cmd_ssh(args):
    sess = _sess_or_exit()
    api  = API(sess)

    job = api.get_job(args.job_id)
    if not job:
        sys.exit(1)
    phase = job.get("status", {}).get("phase", "")

    port_cache = PortCache().load()
    ssh_list = resolve_ssh(api, args.job_id, phase, port_cache, detail_hint=job)
    port_cache.save()

    if not ssh_list:
        cprint("[red]该作业暂无 SSH 信息（未运行或不是调试模式）[/red]")
        sys.exit(1)

    if args.task:
        entry = next((e for e in ssh_list if e.get("task") == args.task), None)
        if entry is None:
            cprint(f"[red]未找到任务：{args.task}，可用：{[e['task'] for e in ssh_list]}[/red]")
            sys.exit(1)
    elif len(ssh_list) == 1:
        entry = ssh_list[0]
    else:
        cprint("[bold]可用 SSH 节点：[/bold]")
        for i, e in enumerate(ssh_list, 1):
            cprint(f"  [cyan]{i}.[/cyan] {e['task']}  [dim]{e['url']}[/dim]")
        while True:
            choice = input(f"\n请选择节点 (1-{len(ssh_list)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(ssh_list):
                entry = ssh_list[int(choice) - 1]
                break
            cprint("[red]输入无效，请重试[/red]")

    ssh_base, user, host, port = _build_ssh_cmd(
        ssh_list, task=args.task,
        identityfile=getattr(args, "identityfile", None),
        ssh_opts=getattr(args, "ssh_opts", None),
    )
    ssh_cmd = ssh_base + [f"{user}@{host}"]
    # ssh mode: no remote command (interactive shell), drop BatchMode
    ssh_cmd = [a for a in ssh_cmd if a != "BatchMode=yes"]

    cprint(f"[dim]连接：{entry['task']}  {' '.join(ssh_cmd)}[/dim]")
    os.execvp("ssh", ssh_cmd)
