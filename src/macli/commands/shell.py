"""shell 命令：通过 CloudShell WebSocket 进入交互式终端"""
import os, sys, time, threading, tty, termios, select, signal, socket

from macli.constants import console
from macli.log import cprint, dprint, _raw_debug, _status_debug, is_verbose
from macli.helpers import _json_out
from macli.session import _sess_or_exit, API
from macli.websocket import _open_exec_ws, _ws_read_frame, _ws_send_frame, _send_resize
from macli.commands.log_cmd import _pick_log_task


def cmd_shell(args):
    sess = _sess_or_exit()
    api  = API(sess)

    status = api.get_exec_status(args.job_id)
    if status and isinstance(status, dict):
        access = (status.get("access") or {}).get("allow")
        if access is False:
            cprint("[red]该作业当前不允许打开 CloudShell[/red]")
            sys.exit(1)

    tasks = api.get_job_tasks(args.job_id)
    task_name = _pick_log_task(tasks, preferred=args.task, interactive=True)

    dprint("[cyan]正在连接 CloudShell...[/cyan]")
    sock = _open_exec_ws(sess, args.job_id, task_name, command="/bin/bash")
    dprint("[green]\u2713 已连接[/green] [dim](退出热键: Ctrl-])[/dim]")
    dprint(f"[dim][shell] heartbeat interval = {max(0.5, float(args.heartbeat))}s[/dim]")

    stop = {"value": False}
    old_tty = termios.tcgetattr(sys.stdin.fileno())

    def reader():
        try:
            while not stop["value"]:
                try:
                    opcode, payload = _ws_read_frame(sock)
                except TimeoutError:
                    continue
                except socket.timeout:
                    continue
                if opcode == 8:
                    _raw_debug("websocket close frame received")
                    break
                if opcode in (1, 2) and payload:
                    # cloudShell 下行目前已确认主要是 0x01 + 终端字节流
                    if opcode == 2 and payload[:1] == b"\x01":
                        _status_debug(f"recv frame: opcode={opcode} ch=01 len={len(payload)-1}")
                        payload = payload[1:]
                    else:
                        _status_debug(f"recv frame: opcode={opcode} raw-len={len(payload)} head={payload[:8].hex()}")
                    if payload:
                        os.write(sys.stdout.fileno(), payload)
                        sys.stdout.flush()
        except Exception as e:
            _raw_debug(f"reader error: {type(e).__name__}: {e}")
        stop["value"] = True

    _heart_toggle = {"on": True}

    def _blink_heart():
        sym = "\u2665" if _heart_toggle["on"] else "\u2661"
        _heart_toggle["on"] = not _heart_toggle["on"]
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 80
        # Save cursor -> jump to top-right -> print heart -> restore cursor
        indicator = f"\033[s\033[1;{cols}H\033[31m{sym}\033[m\033[u"
        os.write(sys.stderr.fileno(), indicator.encode())

    def heartbeat_sender():
        try:
            interval = max(0.5, float(args.heartbeat))
            while not stop["value"]:
                time.sleep(interval)
                if stop["value"]:
                    break
                try:
                    _ws_send_frame(sock, b"\x00", opcode=2)
                    if is_verbose():
                        _blink_heart()
                except Exception as e:
                    _raw_debug(f"heartbeat failed: {type(e).__name__}: {e}")
                    stop["value"] = True
                    break
        except Exception as e:
            _raw_debug(f"heartbeat thread error: {type(e).__name__}: {e}")

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    hb = threading.Thread(target=heartbeat_sender, daemon=True)
    hb.start()

    old_sigwinch = signal.getsignal(signal.SIGWINCH)

    def _on_resize(signum, frame):
        try:
            sz = os.get_terminal_size()
            _send_resize(sock, sz.columns, sz.lines)
        except Exception:
            pass

    try:
        tty.setraw(sys.stdin.fileno())
        signal.signal(signal.SIGWINCH, _on_resize)
        try:
            sz = os.get_terminal_size()
            _send_resize(sock, sz.columns, sz.lines)
            _raw_debug(f"initial resize sent: {sz.columns}x{sz.lines}")
        except Exception as e:
            _raw_debug(f"resize send failed: {type(e).__name__}: {e}")
        # 打开 stdin 通道；不主动补回车，避免重复打印 prompt
        try:
            _ws_send_frame(sock, b"\x00", opcode=2)
            _raw_debug("init stdin frame sent")
        except Exception as e:
            _raw_debug(f"init send failed: {type(e).__name__}: {e}")
        while not stop["value"]:
            r, _, _ = select.select([sys.stdin.fileno()], [], [], 0.1)
            if not r:
                continue
            data = os.read(sys.stdin.fileno(), 1)
            if not data:
                break
            # cloudShell 上行消息格式：0x00 + stdin字节
            _ws_send_frame(sock, b"\x00" + data, opcode=2)
    finally:
        signal.signal(signal.SIGWINCH, old_sigwinch)
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_tty)
        try:
            _ws_send_frame(sock, b"\x00exit\r", opcode=2)
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass
        dprint("\n[dim]CloudShell 已退出[/dim]")
