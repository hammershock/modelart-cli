import sys
from pathlib import Path
from macli.constants import console, Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from macli.log import cprint, dprint
from macli.helpers import _json_out
from macli.session import _sess_or_exit, API


def _pick_log_task(tasks: list, preferred: str = None, interactive: bool = False) -> str:
    if preferred:
        return preferred
    if not tasks:
        return "worker-0"
    if len(tasks) == 1:
        return tasks[0].get("task") or "worker-0"
    if not interactive:
        chosen = tasks[0]
        task = chosen.get("task") or "worker-0"
        ip   = chosen.get("ip", "")
        dprint(f"[dim]自动选择任务：{task} {ip}（共 {len(tasks)} 个节点，可用 --task 指定）[/dim]")
        return task
    cprint("[bold]可用任务：[/bold]")
    for i, item in enumerate(tasks, 1):
        task = item.get("task", "")
        ip = item.get("ip", "")
        host_ip = item.get("host_ip", "")
        cprint(f"  [cyan]{i}.[/cyan] {task} [dim]{ip} / {host_ip}[/dim]")
    while True:
        choice = input(f"\n请选择任务 (1-{len(tasks)}): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(tasks):
            return tasks[int(choice) - 1].get("task") or "worker-0"
        cprint("[red]输入无效，请重试[/red]")


def _save_response_with_progress(resp, outpath: Path) -> int:
    total = int(resp.headers.get("content-length") or 0)
    size = 0
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with outpath.open("wb") as f:
        if total > 0:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("下载日志", total=total)
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    f.write(chunk)
                    n = len(chunk)
                    size += n
                    progress.update(task, advance=n)
        else:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("下载日志", total=None)
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    f.write(chunk)
                    n = len(chunk)
                    size += n
                    progress.update(task, advance=n)
    return size


def cmd_log(args):
    sess = _sess_or_exit()
    api  = API(sess)

    if not args.output:
        cprint("[red]请显式指定 --output <OUTPUT_PATH>[/red]")
        sys.exit(1)

    tasks = api.get_job_tasks(args.job_id)
    task_name = _pick_log_task(tasks, preferred=args.task)

    if getattr(args, "json", False):
        _json_out({
            "job_id": args.job_id,
            "task": task_name,
            "tasks": tasks,
        })
        return

    info = api.get_job_log_url(args.job_id, task_name)
    obs_url = info.get("obs_url", "")
    if not obs_url:
        cprint("[red]未获取到日志下载地址[/red]")
        sys.exit(1)

    outpath = Path(args.output).expanduser()
    if outpath.is_dir():
        outpath = outpath / f"{args.job_id[:8]}_{task_name}.log"
    resp = api.download_from_obs_url(obs_url, timeout=args.timeout)
    if resp.status_code != 200:
        cprint(f"[red]日志下载失败 {resp.status_code}[/red]")
        sys.exit(1)
    size = _save_response_with_progress(resp, outpath)

    cprint(f"[green]✓ 日志已导出[/green] {outpath} [dim]({size} bytes)[/dim]")
    cprint(f"[dim]job={args.job_id}  task={task_name}[/dim]")
