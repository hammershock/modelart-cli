"""WebSocket 帧收发 & CloudShell exec 连接"""
import os, json, ssl, socket, base64, struct, urllib.parse

from macli.log import _raw_debug


def _ws_recv_exact(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("socket closed")
        buf += chunk
    return buf


def _ws_read_frame(sock):
    b1, b2 = _ws_recv_exact(sock, 2)
    opcode = b1 & 0x0F
    masked = (b2 >> 7) & 1
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", _ws_recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _ws_recv_exact(sock, 8))[0]
    mask = b""
    if masked:
        mask = _ws_recv_exact(sock, 4)
    payload = _ws_recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def _ws_send_frame(sock, payload: bytes, opcode: int = 2):
    fin_opcode = 0x80 | (opcode & 0x0F)
    mask_bit = 0x80
    n = len(payload)
    header = bytearray([fin_opcode])
    if n < 126:
        header.append(mask_bit | n)
    elif n < (1 << 16):
        header.append(mask_bit | 126)
        header.extend(struct.pack("!H", n))
    else:
        header.append(mask_bit | 127)
        header.extend(struct.pack("!Q", n))
    mask_key = os.urandom(4)
    header.extend(mask_key)
    masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    sock.sendall(bytes(header) + masked)


def _open_exec_ws(sess, job_id: str, task_name: str, command: str = "/bin/bash"):
    host = "console.huaweicloud.com"
    path = (
        f"/modelarts/rest/v2/{sess.project_id}/training-jobs/{job_id}/exec"
        f"?task_id={urllib.parse.quote(task_name)}&command={urllib.parse.quote(command)}"
    )
    proto = (
        f"origin|https%3A%2F%2Fconsole.huaweicloud.com, "
        f"cftk|{sess.cftk or ''}, "
        f"agencyid|{sess.agency_id or ''}, "
        f"projectname|{sess.region or ''}, "
        f"region|{sess.region or ''}"
    )
    key = base64.b64encode(os.urandom(16)).decode()
    cookie = "; ".join(f"{c.name}={c.value}" for c in sess.http.cookies)
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Protocol: {proto}\r\n"
        f"Origin: https://console.huaweicloud.com\r\n"
        f"User-Agent: Mozilla/5.0\r\n"
        f"Cookie: {cookie}\r\n"
        f"\r\n"
    )
    ctx = ssl.create_default_context()
    raw_sock = socket.create_connection((host, 443), timeout=10)
    sock = ctx.wrap_socket(raw_sock, server_hostname=host)
    sock.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(4096)
    header = resp.split(b"\r\n\r\n", 1)[0].decode("utf-8", errors="replace")
    if "101 Switching Protocols" not in header:
        sock.close()
        raise RuntimeError(f"CloudShell websocket 握手失败: {header[:300]}")
    sock.settimeout(2)
    return sock


def _send_resize(sock, cols: int, rows: int):
    msg = json.dumps({"Width": cols, "Height": rows}).encode()
    _ws_send_frame(sock, b"\x04" + msg, opcode=2)
