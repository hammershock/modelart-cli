import sys
from macli.config import load_session, save_session, _clear_saved_creds, _config_path
from macli.log import cprint, dprint
from macli.auth import (_extract_cftk, _get_cookie_from_args_or_input,
                        _setup_session_from_cookie, _manual_cookie_input)


def cmd_login(args):
    # 登录命令始终重新认证，不复用缓存 cookie（避免直接返回过期的旧 cookie）
    if not getattr(args, "cookie", None):
        d = load_session()
        d.pop("cookie_str", None)
        save_session(d)

    ck, http_session = _get_cookie_from_args_or_input(args)

    # 登录失败时，只有 --cookie 路径才继续
    if not ck:
        if not getattr(args, "cookie", None):
            sys.exit(1)
        # --cookie 传了但为空，展示指南让用户粘贴
        ck = _manual_cookie_input()
        http_session = None
        if not ck:
            sys.exit(1)

    # 验证 cftk，失败时展示指南让用户重新粘贴
    if not _extract_cftk(ck):
        cprint("[red]cookie 无效（无法提取 cftk），请重新获取[/red]")
        ck = _manual_cookie_input()
        http_session = None
        if not ck:
            sys.exit(1)
        if not _extract_cftk(ck):
            cprint("[red]cookie 仍然无效，退出[/red]")
            sys.exit(1)

    _setup_session_from_cookie(ck, interactive=getattr(args, "interactive", False),
                               http_session=http_session)
    cprint(f"\n[green]✓ 登录成功！[/green]")


def cmd_logout(args):
    """清除已保存的登录凭据（保留持久化偏好配置）"""
    session_keys = {
        "region", "project_id", "agency_id", "workspace_id", "cftk",
        "cookies", "cookie_str", "saved_at", "ssh_port_cache",
    }

    data = load_session()
    had_session_state = any(k in data for k in session_keys)
    for k in session_keys:
        data.pop(k, None)

    p = _config_path()
    if data:
        save_session(data)
    elif p.exists():
        p.unlink()

    purge = getattr(args, "purge", False)
    cleared_creds = _clear_saved_creds() if purge else False

    if had_session_state or cleared_creds:
        parts = []
        if had_session_state: parts.append("登录 session")
        if cleared_creds:     parts.append("Keychain 账号密码")
        cprint(f"[green]✓ 已清除：{' 及 '.join(parts)}[/green]")
        if data:
            cprint("[dim]已保留其他配置：如 autologin、identityfiles、exec backend 等[/dim]")
        if had_session_state and not purge:
            cprint("[dim]提示：keyring 账号密码已保留（autologin 可复用），"
                   "如需彻底清除请使用 macli logout --purge[/dim]")
    else:
        cprint("[yellow]当前没有已保存的登录凭据[/yellow]")
