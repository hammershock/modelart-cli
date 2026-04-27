import os, sys
from pathlib import Path
from macli.constants import console
from macli.config import load_identityfiles, save_identityfiles
from macli.log import cprint
from rich.table import Table


def cmd_identityfile(args):
    files, default = load_identityfiles()

    if args.if_cmd == "add":
        path = str(Path(args.path).expanduser())
        if not os.path.exists(path):
            cprint(f"[yellow]警告：文件不存在：{path}[/yellow]")
        name = args.name if args.name else Path(path).stem
        if name in files and files[name] != path:
            cprint(f"[yellow]名称 '{name}' 已存在（{files[name]}），将被覆盖[/yellow]")
        files[name] = path
        # 若尚无默认，自动设为第一个添加的
        if default is None:
            default = name
        save_identityfiles(files, default)
        cprint(f"[green]✓ 已添加：{name} → {path}[/green]")
        if default == name:
            cprint(f"[dim]（已设为默认密钥）[/dim]")

    elif args.if_cmd == "remove":
        name = args.name
        if name not in files:
            cprint(f"[red]未找到密钥：{name}[/red]")
            sys.exit(1)
        del files[name]
        if default == name:
            default = next(iter(files), None)
            if default:
                cprint(f"[yellow]默认密钥已更新为：{default}[/yellow]")
            else:
                cprint("[yellow]已无默认密钥[/yellow]")
        save_identityfiles(files, default)
        cprint(f"[green]✓ 已移除：{name}[/green]")

    elif args.if_cmd == "list":
        if not files:
            cprint("[dim]暂无已保存的 SSH 密钥[/dim]")
            return
        t = Table(header_style="bold cyan", show_lines=False)
        t.add_column("名称", style="cyan")
        t.add_column("路径")
        t.add_column("默认", width=4)
        for n, p in files.items():
            mark = "[green]✓[/green]" if n == default else ""
            exists_hint = "" if os.path.exists(Path(p).expanduser()) else " [red](文件不存在)[/red]"
            t.add_row(n, p + exists_hint, mark)
        console.print(t)

    elif args.if_cmd == "default":
        if args.set:
            name_or_path = args.set
            # 若是已知名称
            if name_or_path in files:
                default = name_or_path
            else:
                # 视为路径，自动以文件名为键添加（若未注册）
                path = str(Path(name_or_path).expanduser())
                stem = Path(name_or_path).stem
                if stem not in files:
                    files[stem] = path
                default = stem
            save_identityfiles(files, default)
            cprint(f"[green]✓ 默认密钥已设为：{default} ({files.get(default, name_or_path)})[/green]")
        else:
            if default and default in files:
                cprint(f"默认密钥：[cyan]{default}[/cyan] → {files[default]}")
            elif default:
                cprint(f"默认密钥名称：[cyan]{default}[/cyan]（未在列表中，将直接用作路径）")
            else:
                cprint("[dim]未设置默认密钥，可用 macli identityfile default --set <PATH/NAME>[/dim]")
