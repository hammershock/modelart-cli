"""exec 命令：在作业容器内执行命令（cloudshell / ssh 后端）"""
import os, sys, re, time, threading, socket, subprocess as _subprocess

from macli.constants import console
from macli.config import load_identityfiles, get_exec_backend, set_exec_backend
from macli.log import cprint, dprint, _raw_debug
from macli.helpers import (PortCache, resolve_ssh, resolve_identityfile,
                           _parse_ssh_url, _read_piped_ids)
from macli.session import _sess_or_exit, API
from macli.websocket import _open_exec_ws, _ws_read_frame, _ws_send_frame
from macli.commands.log_cmd import _pick_log_task


def _exec_script(
    sess: "ConsoleSession",
    job_id: str,
    task_name: str,
    script: str,
    timeout: int = 120,
    cwd: str = None,
) -> "tuple[str, int]":
    """
    底层传输：通过 CloudShell WebSocket 执行 script，返回 (stdout文本, exit_code)。
    脚本经 base64 编码传输，支持多行/特殊字符/heredoc 等任意内容。
    """
    import base64 as _b64
    script_b64 = _b64.b64encode(script.encode()).decode()

    START_MARKER = "MACLI_EXEC_START_7f3a9"
    EXIT_MARKER  = "MACLI_EXEC_EXIT_7f3a9"
    TMP_B64      = "/tmp/.macli_exec_b64_$$"

    CHUNK = 512
    chunks = [script_b64[i:i+CHUNK] for i in range(0, len(script_b64), CHUNK)]

    setup_lines = [
        "stty -echo; PS1=''; PS2=''\r",
        f"TMP={TMP_B64}; rm -f \"$TMP\"\r",
    ]
    for ch in chunks:
        setup_lines.append(f"printf '%s' '{ch}' >> \"$TMP\"\r")

    run_parts = ["base64 -d \"$TMP\" | bash"]
    if cwd:
        cwd_esc = cwd.replace("'", "'\\''")
        run_parts = [f"cd '{cwd_esc}' &&"] + run_parts

    setup_lines.append(
        f"echo {START_MARKER}; "
        + " ".join(run_parts)
        + f"; echo {EXIT_MARKER}:$?; rm -f \"$TMP\"; exit\r"
    )

    sock = _open_exec_ws(sess, job_id, task_name, command="/bin/bash")

    buf       = bytearray()
    exit_code = [None]
    done      = threading.Event()

    def _reader():
        try:
            while not done.is_set():
                try:
                    opcode, payload = _ws_read_frame(sock)
                except (TimeoutError, socket.timeout):
                    continue
                if opcode == 8:
                    break
                if opcode in (1, 2) and payload:
                    if opcode == 2 and payload[:1] == b"\x01":
                        payload = payload[1:]
                    if payload:
                        buf.extend(payload)
                        if EXIT_MARKER.encode() in buf:
                            m = re.search(rf"{EXIT_MARKER}:(\d+)",
                                          buf.decode("utf-8", errors="replace"))
                            if m:
                                exit_code[0] = int(m.group(1))
                            done.set()
        except Exception as e:
            _raw_debug(f"_exec_script reader: {type(e).__name__}: {e}")
        done.set()

    def _heartbeat():
        while not done.is_set():
            done.wait(timeout=5)
            if done.is_set():
                break
            try:
                _ws_send_frame(sock, b"\x00", opcode=2)
            except Exception:
                break

    threading.Thread(target=_reader,    daemon=True).start()
    threading.Thread(target=_heartbeat, daemon=True).start()

    time.sleep(0.8)
    for line in setup_lines:
        _ws_send_frame(sock, b"\x00" + line.encode(), opcode=2)
        time.sleep(0.05)

    done.wait(timeout=timeout)
    if not done.is_set():
        _raw_debug(f"_exec_script timeout after {timeout}s")

    try:
        sock.close()
    except Exception:
        pass

    raw = buf.decode("utf-8", errors="replace")
    if START_MARKER in raw:
        raw = raw.split(START_MARKER, 1)[1].lstrip("\r\n")
    if EXIT_MARKER in raw:
        raw = raw[:raw.index(EXIT_MARKER)]
    clean = re.sub(r"\x1b\[[0-9;]*[A-Za-z]|\r", "", raw)

    return clean, exit_code[0] if exit_code[0] is not None else -1


def _build_ssh_cmd(ssh_entries: list, task: str = None,
                   identityfile: str = None, ssh_opts: list = None) -> "tuple[list, str, str, int]":
    """构造 SSH 命令基础参数，返回 (ssh_base_cmd, user, host, port)。
    ssh_entries 为已 enrich 的列表（来自 resolve_ssh）。
    """
    if not ssh_entries:
        cprint("[red]该作业暂无 SSH 信息，无法使用 SSH 后端[/red]")
        sys.exit(1)
    if task:
        entry = next((e for e in ssh_entries if e.get("task") == task), None)
        if entry is None:
            cprint(f"[red]未找到任务：{task}，可用：{[e['task'] for e in ssh_entries]}[/red]")
            sys.exit(1)
    else:
        entry = ssh_entries[0]
        if len(ssh_entries) > 1:
            dprint(f"[dim]自动选择 {entry['task']}（共 {len(ssh_entries)} 个节点，可用 --task 指定）[/dim]")
    user, host, port = _parse_ssh_url(entry["url"])
    if not host:
        cprint(f"[red]无法解析 SSH URL：{entry['url']}[/red]")
        sys.exit(1)
    if not identityfile:
        _, default = load_identityfiles()
        identityfile = default
    if not identityfile:
        cprint("[red]未指定 SSH 密钥，请用 --identityfile 或 macli identityfile default --set <PATH>[/red]")
        sys.exit(1)
    identity_path = resolve_identityfile(identityfile)
    cmd = ["ssh", "-p", str(port), "-i", identity_path,
           "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
           "-o", "LogLevel=ERROR", "-o", "BatchMode=yes"]
    if ssh_opts:
        cmd += ssh_opts
    return cmd, user, host, port


def _exec_script_ssh_capture(
    ssh_entries: list,
    script: str,
    task: str = None,
    timeout: int = 300,
    cwd: str = None,
    identityfile: str = None,
    ssh_opts: list = None,
) -> "tuple[str, int]":
    """通过 SSH 执行脚本并捕获输出，返回 (stdout文本, exit_code)。用于 probe 等需要解析输出的场景。"""
    ssh_base, user, host, _ = _build_ssh_cmd(ssh_entries, task=task,
                                              identityfile=identityfile, ssh_opts=ssh_opts)
    if cwd:
        cwd_esc = cwd.replace("'", "'\\''")
        remote_cmd = f"cd '{cwd_esc}' && bash -s"
    else:
        remote_cmd = "bash -s"
    cmd = ssh_base + [f"{user}@{host}", remote_cmd]
    dprint(f"[dim]{' '.join(cmd)}[/dim]")
    try:
        result = _subprocess.run(cmd, input=script.encode(),
                                 stdout=_subprocess.PIPE, stderr=_subprocess.PIPE,
                                 timeout=timeout)
        return result.stdout.decode("utf-8", errors="replace"), result.returncode
    except _subprocess.TimeoutExpired:
        return "", -1
    except Exception as e:
        dprint(f"[red]SSH capture 失败: {e}[/red]")
        return "", -1


def _exec_script_ssh(
    ssh_entries: list,
    script: str,
    task: str = None,
    timeout: int = 300,
    cwd: str = None,
    identityfile: str = None,
    ssh_opts: list = None,
) -> int:
    """通过原生 SSH 执行脚本，stdout/stderr 直接流向终端，返回 exit_code。"""
    ssh_base, user, host, _ = _build_ssh_cmd(ssh_entries, task=task,
                                              identityfile=identityfile, ssh_opts=ssh_opts)
    if cwd:
        cwd_esc = cwd.replace("'", "'\\''")
        remote_cmd = f"cd '{cwd_esc}' && bash -s"
    else:
        remote_cmd = "bash -s"
    ssh_cmd = ssh_base + [f"{user}@{host}", remote_cmd]
    dprint(f"[dim]{' '.join(ssh_cmd)}[/dim]")
    try:
        result = _subprocess.run(ssh_cmd, input=script.encode(), timeout=timeout)
        return result.returncode
    except _subprocess.TimeoutExpired:
        cprint(f"[red]SSH 执行超时（{timeout}s）[/red]")
        return -1
    except Exception as e:
        cprint(f"[red]SSH 执行失败: {e}[/red]")
        return -1


def _exec_one(args, sess, api, job_id: str, backend: str, script: str) -> int:
    """对单个 job_id 执行脚本，返回退出码。"""
    timeout = getattr(args, "timeout", 300)
    cwd     = getattr(args, "cwd", None)
    task    = getattr(args, "task", None)

    dprint(f"[dim]_exec_one: job={job_id[:8]}... backend={backend} cwd={cwd} timeout={timeout}[/dim]")

    if backend == "ssh":
        job = api.get_job(job_id)
        if not job:
            return 1
        phase = job.get("status", {}).get("phase", "")
        port_cache = PortCache().load()
        ssh_entries = resolve_ssh(api, job_id, phase, port_cache, detail_hint=job)
        port_cache.save()
        if not ssh_entries:
            cprint("[red]该作业暂无 SSH 信息，无法使用 SSH 后端[/red]")
            return 1
        return _exec_script_ssh(
            ssh_entries, script,
            task=task, timeout=timeout, cwd=cwd,
            identityfile=getattr(args, "identityfile", None),
            ssh_opts=getattr(args, "ssh_opts", None),
        )

    status = api.get_exec_status(job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业当前不允许执行命令（CloudShell 未就绪）[/red]")
            return 1

    tasks     = api.get_job_tasks(job_id)
    task_name = _pick_log_task(tasks, preferred=task)
    dprint(f"[cyan]正在连接（task={task_name}）...[/cyan]")
    output, code = _exec_script(sess, job_id, task_name, script, timeout=timeout, cwd=cwd)
    dprint("[green]\u2713 完成[/green]")
    sys.stdout.write(output)
    if output and not output.endswith("\n"):
        sys.stdout.write("\n")
    return code


def _exec_batch(args, backend: str, job_ids: list):
    """对多个作业顺序执行同一脚本（由管道传入 ID）。"""
    sess = _sess_or_exit()
    api  = API(sess)

    # 读取脚本内容（脚本来源不含 stdin，已由调用方保证）
    if getattr(args, "script_file", None):
        try:
            with open(args.script_file, "r") as f:
                script = f.read()
        except OSError as e:
            cprint(f"[red]读取脚本文件失败: {e}[/red]"); sys.exit(1)
    elif getattr(args, "inline_cmd", None):
        parts = args.inline_cmd
        if parts and parts[0] == "--":
            parts = parts[1:]
        script = " ".join(parts)
    else:
        cprint("[red]批量模式下请使用 -- <cmd> 或 --script 指定命令[/red]"); sys.exit(1)

    dprint(f"[dim]_exec_batch: {len(job_ids)} 个作业，backend={backend}[/dim]")
    exit_codes = []
    for job_id in job_ids:
        job = api.get_job(job_id)
        label = job.get("metadata", {}).get("name", job_id) if job else job_id
        cprint(f"\n[bold cyan]══ {label} ({job_id[:8]}...) ══[/bold cyan]")
        code = _exec_one(args, sess, api, job_id, backend, script)
        dprint(f"[dim]  {label}: exit {code}[/dim]")
        exit_codes.append(code)

    failed = sum(1 for c in exit_codes if c != 0)
    if failed:
        cprint(f"\n[yellow]完成：{len(job_ids) - failed}/{len(job_ids)} 个作业成功[/yellow]")
        sys.exit(1)
    else:
        cprint(f"\n[green]完成：全部 {len(job_ids)} 个作业执行成功[/green]")


def cmd_exec(args):
    """在作业容器内执行命令，支持 cloudshell 和 ssh 两种后端。"""

    # -- 确定并记忆后端 --
    backend_arg = getattr(args, "backend", None)
    if backend_arg:
        set_exec_backend(backend_arg)
        backend = backend_arg
    else:
        backend = get_exec_backend()

    # 无 JOB_ID：检查是否有管道 ID（且有内联命令），否则仅保存后端设置
    if not getattr(args, "job_id", None):
        # --stdin 读脚本与管道读 ID 互斥：有 --stdin 时 stdin 留给脚本
        piped_ids = [] if getattr(args, "use_stdin", False) else _read_piped_ids()
        has_cmd   = (getattr(args, "script_file", None)
                     or getattr(args, "use_stdin", False)
                     or getattr(args, "inline_cmd", None))
        if piped_ids and has_cmd:
            # 批量模式：对每个 JOB_ID 顺序执行同一条命令
            args.job_id = None          # 占位，下面按 job_id 循环
            _exec_batch(args, backend, piped_ids)
            return
        if backend_arg:
            cprint(f"[green]\u2713 默认 exec 后端已设为：{backend}[/green]")
        else:
            cprint(f"当前 exec 后端：[cyan]{backend}[/cyan]")
        return

    sess = _sess_or_exit()
    api  = API(sess)

    # -- 确定脚本内容 --
    if getattr(args, "script_file", None):
        try:
            with open(args.script_file, "r") as f:
                script = f.read()
        except OSError as e:
            cprint(f"[red]读取脚本文件失败: {e}[/red]")
            sys.exit(1)
    elif getattr(args, "use_stdin", False):
        script = sys.stdin.read()
    elif getattr(args, "inline_cmd", None):
        parts = args.inline_cmd
        if parts and parts[0] == "--":
            parts = parts[1:]
        script = " ".join(parts)
    else:
        cprint("[red]请指定命令：使用 -- <cmd>、--script <file> 或 --stdin[/red]")
        sys.exit(1)

    sys.exit(_exec_one(args, sess, api, args.job_id, backend, script))
