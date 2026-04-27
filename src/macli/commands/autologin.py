import sys, os, time, uuid

from macli.constants import console, Confirm
from macli.config import (load_session, save_session, load_auto_login_cfg, save_auto_login_cfg,
                          _load_saved_creds, _save_creds, _clear_saved_creds, _KEYRING_OK)
from macli.log import cprint, dprint


def _autologin_print_cfg(cfg: dict):
    """打印自动登录配置摘要。"""
    webhook_url = cfg.get("webhook_url", "")
    ntfy_topic  = cfg.get("ntfy_topic", "")
    if webhook_url:
        cprint(f"  OTP 通道  : [cyan]webhook → {webhook_url}[/cyan]")
    elif ntfy_topic:
        cprint(f"  OTP 通道  : [cyan]ntfy → {ntfy_topic}[/cyan]")
    else:
        cprint(f"  OTP 通道  : [dim]未配置[/dim]")
    cprint(f"  最大重试次数: {cfg.get('max_retries', 3)}")
    cprint(f"  验证码等待 : {cfg.get('otp_wait_secs', 120)} 秒")
    cprint(f"  熔断阈值   : {cfg.get('circuit_breaker', 3)} 次连续失败")


def cmd_autologin(args):
    """查询/启用/停用会话过期时的自动重新登录（keyring 账号密码 + webhook/ntfy OTP 通道）。"""
    import getpass as _getpass
    import secrets as _secrets

    action = getattr(args, "action", None) or "status"

    # ── status (with optional inline config update) ───────────
    if action == "status":
        cfg = load_auto_login_cfg()
        changed = False
        if getattr(args, "retries", None) is not None:
            cfg["max_retries"] = args.retries
            changed = True
        if getattr(args, "timeout", None) is not None:
            cfg["otp_wait_secs"] = args.timeout
            changed = True
        if getattr(args, "circuit_breaker", None) is not None:
            cfg["circuit_breaker"] = args.circuit_breaker
            changed = True
        if getattr(args, "reset_topic", False):
            cfg["ntfy_topic"] = "macli-" + _secrets.token_hex(8)
            changed = True
        if changed:
            save_auto_login_cfg(cfg)
            cprint("[green]✓ 配置已更新[/green]")
        if not cfg.get("enabled"):
            if cfg.get("circuit_tripped"):
                n = cfg.get("consecutive_failures", 0)
                cprint(f"[red]自动登录：[bold]已禁用（熔断 — 连续失败 {n} 次）[/bold][/red]")
            else:
                cprint("[yellow]自动登录：[bold]未启用[/bold][/yellow]")
        else:
            cprint("[green]自动登录：[bold]已启用[/bold][/green]")
            _autologin_print_cfg(cfg)
            failures = int(cfg.get("consecutive_failures", 0))
            if failures > 0:
                cprint(f"  [yellow]当前连续失败: {failures} 次[/yellow]")
        creds = _load_saved_creds()
        if creds.get("username"):
            cprint(f"  keyring 账号: [dim]{creds['username']} @ {creds['domain']}[/dim]")
        else:
            cprint("  keyring 账号: [dim]未保存[/dim]")
        return

    # ── disable ───────────────────────────────────────────────
    if action == "disable":
        cfg = load_auto_login_cfg()
        cfg["enabled"] = False
        save_auto_login_cfg(cfg)
        cprint("[green]✓ 自动登录已停用[/green]")
        return

    # ── enable ────────────────────────────────────────────────
    cfg = load_auto_login_cfg()

    # Already enabled (or re-enabling after circuit trip): apply params, reset circuit state
    if cfg.get("enabled") or cfg.get("circuit_tripped"):
        changed = False
        if getattr(args, "retries", None) is not None:
            cfg["max_retries"] = args.retries
            changed = True
        if getattr(args, "timeout", None) is not None:
            cfg["otp_wait_secs"] = args.timeout
            changed = True
        if getattr(args, "circuit_breaker", None) is not None:
            cfg["circuit_breaker"] = args.circuit_breaker
            changed = True
        if getattr(args, "reset_topic", False):
            cfg["ntfy_topic"] = "macli-" + _secrets.token_hex(8)
            changed = True
        # Always clear circuit state when enable is explicitly called
        if cfg.get("circuit_tripped") or cfg.get("consecutive_failures", 0):
            cfg["circuit_tripped"]      = False
            cfg["consecutive_failures"] = 0
            cfg["enabled"]              = True
            changed = True
        if changed:
            save_auto_login_cfg(cfg)
            cprint("[green]✓ 配置已更新[/green]")
        else:
            cprint("[dim]自动登录已启用，无参数变更[/dim]")
        _autologin_print_cfg(cfg)
        creds = _load_saved_creds()
        if creds.get("username"):
            cprint(f"  keyring 账号: [dim]{creds['username']} @ {creds['domain']}[/dim]")
        return

    # Not yet enabled: full setup flow
    creds = _load_saved_creds()
    if not (creds.get("domain") and creds.get("username") and creds.get("password")):
        cprint("[yellow]keyring 中无账号密码，请输入：[/yellow]")
        _domain   = input("租户名/原华为云账号: ").strip()
        _username = input("IAM 用户名/邮件地址: ").strip()
        _password = _getpass.getpass("IAM 用户密码: ")
        if not all([_domain, _username, _password]):
            cprint("[red]账号信息不完整，取消[/red]")
            return
        if _save_creds(_domain, _username, _password):
            cprint("[green]✓ 账号密码已保存至 keyring[/green]")
        else:
            cprint("[red]keyring 保存失败，无法启用自动登录[/red]")
            return
    else:
        cprint(f"[green]✓ 使用 keyring 账号：{creds['username']} @ {creds['domain']}[/green]")

    # OTP 通道选择：webhook（推荐）或 ntfy（降级）
    cprint("\n[bold]选择 OTP 通道：[/bold]")
    cprint("  [cyan]1.[/cyan] webhook（推荐 — 手机直接 POST 到 macli server）")
    cprint("  [cyan]2.[/cyan] ntfy.sh（公网中转）")
    otp_choice = input("\n请选择 (1/2) [1]: ").strip() or "1"

    if otp_choice == "1":
        # webhook 模式
        existing_url = cfg.get("webhook_url", "")
        default_url = existing_url or "http://localhost:8086"
        cprint(f"\n[dim]填写手机可访问的 macli server 地址（SSH 隧道/内网穿透/Tailscale 均可）[/dim]")
        webhook_url = input(f"webhook URL [{default_url}]: ").strip() or default_url
        # 清理末尾斜杠
        webhook_url = webhook_url.rstrip("/")
        ntfy_topic = cfg.get("ntfy_topic", "")  # 保留旧的，不再生成新的
    else:
        # ntfy 模式
        webhook_url = ""
        existing_topic = cfg.get("ntfy_topic", "")
        if getattr(args, "reset_topic", False) or not existing_topic:
            ntfy_topic = "macli-" + _secrets.token_hex(8)
            dprint(f"[dim]生成新 ntfy topic: {ntfy_topic}[/dim]")
        else:
            ntfy_topic = existing_topic
            dprint(f"[dim]复用已有 ntfy topic: {ntfy_topic}[/dim]")

    max_retries     = getattr(args, "retries",          None) or int(cfg.get("max_retries",    3))
    otp_timeout     = getattr(args, "timeout",          None) or int(cfg.get("otp_wait_secs", 120))
    circuit_breaker = getattr(args, "circuit_breaker",  None) or int(cfg.get("circuit_breaker", 3))

    cfg.update({
        "enabled":              True,
        "webhook_url":          webhook_url,
        "ntfy_topic":           ntfy_topic,
        "max_retries":          max_retries,
        "otp_wait_secs":        otp_timeout,
        "circuit_breaker":      circuit_breaker,
        "consecutive_failures": 0,
        "circuit_tripped":      False,
    })
    save_auto_login_cfg(cfg)
    cprint("[bold green]✓ 自动登录已启用[/bold green]")

    # 打印 iPhone 快捷指令配置指南
    if webhook_url:
        otp_post_url = f"{webhook_url}/otp"
        console.print(Panel(
            f"[bold]iPhone 快捷指令配置[/bold]\n\n"
            f"  1. 自动化 → 收到含「验证码」的短信时触发\n"
            f"  2. 获取文本 → 将快捷指令输入转为纯文本\n"
            f"  3. 获取URL内容:\n"
            f"       POST  [cyan]{otp_post_url}[/cyan]\n"
            f"       请求体 = [yellow]整条短信原文[/yellow]\n\n"
            f"[dim]macli 自动从短信中提取 6 位验证码，手机端无需做正则。[/dim]\n\n"
            f"[bold]验证：[/bold]curl -X POST {otp_post_url} -d '123456'",
            title="[bold]自动登录 — Webhook 配置[/bold]",
            border_style="cyan",
            padding=(1, 2),
        ))
    else:
        ntfy_url = f"https://ntfy.sh/{ntfy_topic}"
        ntfy_publish_url = "https://ntfy.sh"
        console.print(Panel(
            f"[bold]iPhone 快捷指令配置（推荐：整条短信原文直传）[/bold]\n\n"
            f"[bold cyan]推荐 URL[/bold cyan]\n"
            f"  {ntfy_url}\n\n"
            f"[bold cyan]方法[/bold cyan]\n"
            f"  POST\n\n"
            f"[bold cyan]Headers（可选）[/bold cyan]\n"
            f"  Content-Type  →  text/plain; charset=utf-8\n\n"
            f"[bold cyan]请求体[/bold cyan]\n"
            f"  [yellow]<整条短信原文 / 转成纯文本后的快捷指令输入>[/yellow]\n\n"
            f"[dim]macli 会在收到的消息正文里自动提取首个 6 位数字验证码，无需手机端先做正则。[/dim]\n\n"
            f"---\n\n"
            f"[bold]建议快捷指令流程：[/bold]\n"
            f"  1. 触发条件：收到含「验证码」的短信\n"
            f"  2. 用 [获取文本] 把「快捷指令输入」转换成纯文本\n"
            f"  3. [获取URL内容] → POST {ntfy_url}\n"
            f"     Header: Content-Type = text/plain; charset=utf-8\n"
            f"     请求体: [yellow]<上一步得到的整条短信文本>[/yellow]\n\n"
            f"[bold]备用方案（若你更想发 JSON）[/bold]\n"
            f"  URL: {ntfy_publish_url}\n"
            f"  Header: Content-Type = application/json\n"
            f"  JSON 请求体: topic = [cyan]{ntfy_topic}[/cyan]\n"
            f"                 message = [yellow]<整条短信文本>[/yellow]",
            title="[bold]自动登录 — 手机快捷指令配置指南[/bold]",
            border_style="cyan",
            padding=(1, 2),
        ))
        cprint(f"\n[dim]配置已保存。ntfy topic 请妥善保管（泄露后他人可读取验证码）[/dim]")
