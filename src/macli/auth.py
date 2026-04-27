"""认证/登录相关：OTP 轮询、自动登录、session 初始化"""
import os, sys, json, time, re, urllib.parse

import requests

from macli.constants import _CST, REGION_NAMES, _ME_PROBE_REGIONS, SessionExpiredError, console
from macli.config import (load_session, save_session, _load_saved_creds, _save_creds,
                          _clear_saved_creds, load_auto_login_cfg, save_auto_login_cfg,
                          _AUTOLOGIN_KEY)
from macli.log import cprint, dprint, _flog
from macli.net import _new_session


def _ntfy_poll_otp(topic: str, since_ts: int, timeout: int = 120) -> str:
    """
    轮询 ntfy.sh/{topic}，返回第一条在 since_ts 之后发布的 6 位纯数字消息体。
    超时或失败时返回空字符串。
    """
    deadline = time.monotonic() + timeout
    url = f"https://ntfy.sh/{topic}/json"
    seen_ids: set = set()
    dprint(f"[dim]ntfy 轮询开始  url={url}  since={since_ts}[/dim]")
    while time.monotonic() < deadline:
        try:
            r = requests.get(url, params={"poll": "1", "since": str(since_ts)}, timeout=10)
            dprint(f"[dim]ntfy 响应 HTTP {r.status_code}  body={r.text[:120]!r}[/dim]")
            if r.status_code == 200:
                for line in r.text.strip().splitlines():
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line)
                    except Exception:
                        continue
                    mid  = msg.get("id", "")
                    body = (msg.get("message") or "").strip()
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    # 快捷指令有时将 {"message":"123456"} 作为纯文本发送，需要解包
                    if body.startswith("{"):
                        try:
                            inner = json.loads(body)
                            body = (inner.get("message") or body).strip()
                        except Exception:
                            pass
                    dprint(f"[dim]ntfy 消息 id={mid!r} body={body!r}[/dim]")
                    # 支持完整短信原文，从中提取首个 6 位数字
                    m = re.search(r"\b(\d{6})\b", body)
                    if m:
                        return m.group(1)
        except Exception as ex:
            dprint(f"[dim]ntfy 轮询异常: {ex}[/dim]")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(3.0, remaining))
    return ""


def _webhook_poll_otp(webhook_url: str, timeout: int = 120) -> str:
    """
    长轮询本地 macli server /otp/wait 端点，返回 6 位验证码。
    超时或失败时返回空字符串。
    """
    url = webhook_url.rstrip("/") + "/otp/wait"
    dprint(f"[dim]webhook 轮询开始  url={url}  timeout={timeout}[/dim]")
    try:
        r = requests.get(url, params={"timeout": timeout}, timeout=timeout + 10)
        dprint(f"[dim]webhook 响应 HTTP {r.status_code}  body={r.text[:120]!r}[/dim]")
        if r.status_code == 200:
            data = r.json()
            if data.get("ok") and data.get("code"):
                return data["code"]
    except Exception as ex:
        dprint(f"[dim]webhook 轮询异常: {ex}[/dim]")
    return ""


def _do_auto_login(cfg: dict) -> bool:
    """
    用 keyring 中存储的账号密码 + webhook/ntfy OTP 通道自动完成登录，
    成功后更新 session.json 并返回 True，失败返回 False。
    """
    creds = _load_saved_creds()
    if not (creds.get("domain") and creds.get("username") and creds.get("password")):
        cprint("[red]自动登录失败：keyring 中无账号密码，请先执行 macli autologin enable[/red]")
        return False

    webhook_url = cfg.get("webhook_url", "")
    ntfy_topic  = cfg.get("ntfy_topic", "")
    max_retries = int(cfg.get("max_retries", 3))
    otp_timeout = int(cfg.get("otp_wait_secs", 120))

    if not webhook_url and not ntfy_topic:
        cprint("[red]自动登录失败：未配置 webhook_url 或 ntfy_topic，请执行 macli autologin enable[/red]")
        return False

    otp_mode = "webhook" if webhook_url else "ntfy"
    cprint(
        f"\n[bold cyan]⟳ 会话已过期，自动重新登录[/bold cyan]"
        f"  [dim]{creds['username']} @ {creds['domain']}  (OTP: {otp_mode})[/dim]"
    )

    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            cprint(f"[yellow]第 {attempt}/{max_retries} 次重试...[/yellow]")

        # 捕获 poll_since 在闭包外，每次尝试都重新记录
        poll_since = int(time.time())

        if webhook_url:
            def _otp_provider() -> str:
                dprint(f"[dim]webhook 开始轮询，url={webhook_url}，timeout={otp_timeout}s[/dim]")
                cprint(f"[cyan]  ⟳ 等待手机验证码（最多 {otp_timeout} 秒）...[/cyan]")
                code = _webhook_poll_otp(webhook_url, timeout=otp_timeout)
                if not code:
                    cprint("[yellow]  验证码等待超时[/yellow]")
                else:
                    dprint(f"[dim]webhook 收到验证码: {code}[/dim]")
                return code
        else:
            def _otp_provider(since=poll_since) -> str:
                dprint(f"[dim]ntfy 开始轮询，since={since}，topic={ntfy_topic}，timeout={otp_timeout}s[/dim]")
                cprint(f"[cyan]  ⟳ 等待手机验证码（最多 {otp_timeout} 秒）...[/cyan]")
                code = _ntfy_poll_otp(ntfy_topic, since, timeout=otp_timeout)
                if not code:
                    cprint("[yellow]  验证码等待超时[/yellow]")
                else:
                    dprint(f"[dim]ntfy 收到验证码: {code}[/dim]")
                return code

        ck, http_s = _http_login(
            creds["domain"], creds["username"], creds["password"],
            otp_provider=_otp_provider,
        )
        if ck:
            # 保留原有 region/workspace 配置
            old = load_session()
            old_region    = old.get("region", "")
            old_project   = old.get("project_id", "")
            old_agency    = old.get("agency_id", "")
            old_workspace = old.get("workspace_id", "")

            _setup_session_from_cookie(ck, interactive=False, http_session=http_s)

            # 恢复原有 region/workspace（如果之前有配置）
            if old_region:
                d = load_session()
                d["region"]       = old_region
                d["project_id"]   = old_project
                d["agency_id"]    = old_agency
                d["workspace_id"] = old_workspace
                save_session(d)
                dprint(f"[dim]已恢复 region={old_region} workspace={old_workspace}[/dim]")

            cprint("[bold green]✓ 自动重新登录成功[/bold green]")
            return True
        cprint(f"[yellow]第 {attempt} 次尝试失败[/yellow]")

    cprint("[red]✗ 自动重新登录失败，已超过最大重试次数[/red]")
    return False


def _autologin_record_outcome(success: bool) -> bool:
    """
    更新连续失败计数器。
    成功时重置为 0；失败时递增，达到熔断阈值则自动禁用 autologin。
    返回 True 表示本次触发了熔断（autologin 刚被禁用）。
    """
    data = load_session()
    cfg  = data.get(_AUTOLOGIN_KEY, {})
    if success:
        cfg["last_autologin_ts"] = time.time()
        if cfg.get("consecutive_failures", 0) != 0:
            cfg["consecutive_failures"] = 0
            cfg["circuit_tripped"]      = False
        data[_AUTOLOGIN_KEY] = cfg
        save_session(data)
        return False
    threshold = int(cfg.get("circuit_breaker", 3))
    failures  = int(cfg.get("consecutive_failures", 0)) + 1
    cfg["consecutive_failures"] = failures
    if failures >= threshold:
        cfg["enabled"]         = False
        cfg["circuit_tripped"] = True
        data[_AUTOLOGIN_KEY]   = cfg
        save_session(data)
        _flog("ERROR",
              f"autologin 熔断：连续失败 {failures} 次（阈值 {threshold}），已自动禁用")
        return True
    data[_AUTOLOGIN_KEY] = cfg
    save_session(data)
    _flog("WARN", f"autologin 失败（连续 {failures}/{threshold} 次）")
    return False


def _me(http, region, cftk, agency_id="") -> dict:
    """调用 /modelarts/rest/me 获取当前 region 的 projectId"""
    r = http.get(
        "https://console.huaweicloud.com/modelarts/rest/me",
        headers={
            "accept":           "application/json, text/plain, */*",
            "region":           region,
            "projectname":      region,
            "agencyid":         agency_id,
            "x-language":       "zh-cn",
            "x-requested-with": "XMLHttpRequest",
            "x-target-services":"modelarts-iam5",
            "cftk":             cftk,
            "cf2-cftk":         "cftk",
            "user-agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "referer":          f"https://console.huaweicloud.com/modelarts/?locale=zh-cn"
                                f"&agencyId={agency_id}&region={region}",
        },
        timeout=15)
    if r.status_code == 200 and r.text.strip():
        try:
            return r.json()
        except Exception:
            return {}
    return {}


def _me_probe(http, cftk) -> dict:
    """尝试多个常见 region，返回第一个包含 supportRegions 的 me 响应。"""
    for region in _ME_PROBE_REGIONS:
        me = _me(http, region, cftk)
        if me.get("supportRegions"):
            return me
    return {}


def _fetch_workspaces(sess) -> list:
    """调用 workspaces 接口，返回 [{"id":..., "name":...}, ...]"""
    url = (f"https://console.huaweicloud.com/modelarts/rest/v1"
           f"/{sess.project_id}/workspaces"
           f"?offset=0&limit=100&sort_by=update_time&order=desc&name=&filter_accessible=true")
    try:
        r = sess.http.get(url, timeout=15)
        if r.status_code == 200:
            return r.json().get("workspaces", [])
    except Exception as e:
        cprint(f"[yellow]获取工作空间失败: {e}[/yellow]")
    return []


def _select_workspace(sess) -> str:
    """展示工作空间列表，让用户选择，返回选中的 workspace_id"""
    dprint("[cyan]获取工作空间列表...[/cyan]")
    workspaces = _fetch_workspaces(sess)

    if not workspaces:
        cprint("[yellow]未获取到工作空间，请手动输入 Workspace ID[/yellow]")
        return input("Workspace ID: ").strip()

    cprint("\n[bold]可用工作空间：[/bold]")
    for i, ws in enumerate(workspaces, 1):
        status = "" if ws.get("status") == "NORMAL" else f" [{ws.get('status')}]"
        cprint(f"  [cyan]{i}.[/cyan] {ws['name']}{status}")

    while True:
        choice = input(f"\n请选择工作空间 (1-{len(workspaces)}): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(workspaces):
            chosen = workspaces[int(choice) - 1]
            cprint(f"[green]✓ 已选择：{chosen['name']}[/green]")
            return chosen["id"]
        cprint("[red]输入无效，请重试[/red]")


def _http_login(domain: str, username: str, password: str,
                service: str = "https://console.huaweicloud.com/console/",
                otp_provider=None):
    """
    纯 HTTP 登录华为云（IAM 用户 + 短信 MFA），返回 (cookie_str, session)。
    失败返回 ("", None)。
    """
    import urllib.parse, json as _json

    UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

    def q(s):
        return urllib.parse.quote(s, safe="")

    s = _new_session()
    s.headers.update({"User-Agent": UA})

    login_page = (
        f"https://auth.huaweicloud.com/authui/login.html"
        f"?locale=zh-cn&service={q(service)}#/login"
    )
    base_headers = {"User-Agent": UA, "Referer": login_page}

    # Step 1: 打开登录页，获取初始 cookie
    dprint("[dim][1] 打开登录页...[/dim]")
    s.get(login_page, timeout=15)

    # Step 2: login/verify
    dprint("[dim][2] 验证账号...[/dim]")
    s.post(
        "https://auth.huaweicloud.com/authui/login/verify",
        headers=base_headers,
        data={"userName": username, "userType": "name", "accountName": q(domain)},
        timeout=15,
    )

    # Step 3: 密码登录
    dprint("[dim][3] 提交密码...[/dim]")
    payload = {
        "userpasswordcredentials.domain":       q(domain),
        "userpasswordcredentials.domainType":   "name",
        "userpasswordcredentials.username":     q(username),
        "userpasswordcredentials.userInfoType": "name",
        "userpasswordcredentials.countryCode":  "",
        "userpasswordcredentials.verifycode":   "",
        "userpasswordcredentials.password":     q(password),
        "userpasswordcredentials.riskMonitorJson":
            _json.dumps({"devID": "", "hwMeta": ""}, separators=(",", ":")),
        "__checkbox_warnCheck": "true",
        "isAjax":  "true",
        "Submit":  "Login",
    }
    r = s.post(
        "https://auth.huaweicloud.com/authui/validateUser.action",
        headers=base_headers,
        params={"locale": "zh-cn", "service": service},
        data=payload,
        timeout=15,
    )
    try:
        data = r.json()
    except Exception:
        cprint(f"[red]密码登录响应解析失败: {r.text[:200]}[/red]")
        return "", None
    if data.get("loginResult") != "success":
        cprint(f"[red]密码登录失败: {data.get('loginResult')} — {data.get('loginMessage','')}[/red]")
        return "", None
    dprint("[dim]密码验证通过[/dim]")

    # Step 4: 获取 MFA 信息（含 IAMCSRF）
    dprint("[dim][4] 获取 MFA 信息...[/dim]")
    r = s.get(
        "https://auth.huaweicloud.com/authui/getAntiPhishingInfo",
        headers=base_headers,
        params={"isSupport": "false"},
        timeout=15,
    )
    try:
        anti = r.json()
    except Exception:
        cprint(f"[red]MFA 信息获取失败: {r.text[:200]}[/red]")
        return "", None
    if anti.get("result") != "success":
        cprint(f"[red]MFA 信息获取失败: {anti}[/red]")
        return "", None
    iamcsrf = anti.get("IAMCSRF", "")
    dprint(f"[dim]MFA 响应字段: {list(anti.keys())}[/dim]")

    # Step 5: 发送短信验证码
    mfa_referer = (
        "https://auth.huaweicloud.com/authui/loginVerification.html"
        f"?service={q(service)}"
    )
    mfa_headers = {"User-Agent": UA, "Referer": mfa_referer}

    # 手机号字段名因账号类型不同而异，遍历常见字段
    phone = (anti.get("phoneNum") or anti.get("phone") or
             anti.get("mobilePhone") or anti.get("mobile") or
             anti.get("verifyPhone") or "")
    if phone:
        cprint(f"[yellow]验证码将发送至: {phone}[/yellow]")
    else:
        cprint("[yellow]验证码将发送至您绑定的手机[/yellow]")

    if otp_provider is None:
        input("按 Enter 发送验证码...")
    dprint("[dim][5] 发送短信...[/dim]")
    r = s.post(
        "https://auth.huaweicloud.com/authui/sendLoginSms",
        headers=mfa_headers,
        timeout=15,
    )
    try:
        sms_resp = r.json()
    except Exception:
        sms_resp = {}
    if sms_resp.get("result") not in {"success", "faster"}:
        cprint(f"[yellow]短信发送响应: {sms_resp}（继续输入验证码）[/yellow]")
    else:
        cprint("[green]✓ 验证码已发送[/green]")

    # Step 6: 输入并提交验证码
    if otp_provider is not None:
        dprint("[dim][6] 调用 otp_provider 获取验证码...[/dim]")
        sms_code = otp_provider()
    else:
        sys.stdout.flush()
        sms_code = input("\n请输入收到的 6 位验证码: ").strip()
    if not sms_code:
        cprint("[red]验证码不能为空[/red]")
        return "", None

    dprint("[dim][6] 提交验证码...[/dim]")
    r = s.post(
        "https://auth.huaweicloud.com/authui/validateUser",
        headers=mfa_headers,
        params={"locale": "zh-cn", "service": service},
        data={"smsCode": sms_code, "step": "afterAntiPhishing", "IAMCSRF": iamcsrf},
        allow_redirects=False,
        timeout=15,
    )
    if not r.is_redirect:
        cprint(f"[red]验证码提交后未跳转 (HTTP {r.status_code}): {r.text[:200]}[/red]")
        return "", None
    location = r.headers.get("location", "")
    if "actionErrors=419" in location:
        cprint("[red]验证码无效或已失效（419），请重试[/red]")
        return "", None

    next_url = urllib.parse.urljoin(r.url, location)
    dprint(f"[dim][7] 跟随跳转: {next_url[:80]}...[/dim]")

    # Step 7: 跟随重定向链，收集完整 cookie
    for _ in range(10):
        r = s.get(next_url, allow_redirects=False, timeout=15)
        if r.is_redirect and r.headers.get("location"):
            next_url = urllib.parse.urljoin(r.url, r.headers["location"])
        else:
            break

    # 拼出 cookie 字符串（按优先级排序）
    COOKIE_ORDER = [
        "SSOTGC", "SSOJTC", "SID", "J_SESSION_ID", "J_SESSION_REGION",
        "cftk", "console_cftk", "agencyID", "domain_tag", "user_tag",
        "usite", "auth_cdn", "Site", "HWWAFSESID", "HWWAFSESTIME",
        "third-party-access", "x-framework-ob", "xyt_nmk",
    ]
    all_cookies = {c.name: c.value for c in s.cookies}
    ordered = [f"{k}={all_cookies[k]}" for k in COOKIE_ORDER if k in all_cookies]
    remaining = [f"{k}={v}" for k, v in all_cookies.items() if k not in COOKIE_ORDER]
    ck = "; ".join(ordered + remaining)

    if not ck:
        cprint("[yellow]未获取到任何 cookie[/yellow]")
        return "", None

    dprint(f"[dim]获取到 {len(all_cookies)} 个 cookie，共 {len(ck)} 字符[/dim]")
    return ck, s


def _extract_cftk(cookie_str: str) -> str:
    """从 cookie 字符串中提取 cftk 值"""
    for part in cookie_str.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == "cftk":
            return v.strip()
    return ""


def _get_cookie_from_args_or_input(args):
    """按优先级获取 cookie：--cookie 参数 > session 缓存 > 已保存凭据 > 交互登录
    返回 (cookie_str, http_session_or_none)"""
    # 1. --cookie 参数直接给了
    if getattr(args, "cookie", None):
        ck = args.cookie.strip()
        dprint(f"[green]✓ 使用 --cookie 参数（{len(ck)} 字符）[/green]")
        return ck, None

    # 2. 从已保存的 session 中读取 cookie
    d = load_session()
    ck = d.get("cookie_str", "")
    if ck:
        dprint(f"[green]✓ 从 session 读取 cookie（{len(ck)} 字符）[/green]")
        return ck, None

    # 3. 从已保存的账号密码自动登录
    saved = _load_saved_creds()
    if saved.get("domain") and saved.get("username") and saved.get("password"):
        cprint(f"[cyan]使用已保存的账号自动登录：[bold]{saved['username']}[/bold] @ {saved['domain']}[/cyan]")
        ck, http_s = _http_login(saved["domain"], saved["username"], saved["password"])
        if ck:
            dprint("[green]✓ 自动登录成功[/green]")
            return ck, http_s
        cprint("[yellow]已保存的账号登录失败（密码可能已变更），请重新输入[/yellow]")
        _clear_saved_creds()

    # 4. 交互式 HTTP 登录
    import getpass as _getpass
    cprint("[bold cyan]══ 华为云 IAM 登录 ══[/bold cyan]")
    _domain   = input("租户名/原华为云账号: ").strip()
    _username = input("IAM 用户名/邮件地址: ").strip()
    _password = _getpass.getpass("IAM 用户密码: ")

    if not all([_domain, _username, _password]):
        cprint("[red]账号信息不完整[/red]")
        return "", None

    ck, http_s = _http_login(_domain, _username, _password)
    if not ck:
        return "", None

    if _save_creds(_domain, _username, _password):
        cprint("[green]✓ 账号密码已安全保存，下次自动登录[/green]")
    else:
        dprint("[dim]密码未保存（keyring 不可用）[/dim]")

    return ck, http_s


def _setup_session_from_cookie(ck: str, interactive: bool, http_session=None) -> None:
    """用 cookie 初始化 session，interactive=True 时交互选择 region/workspace。
    http_session: 来自 _http_login 的原始 session，有完整的 console 上下文；
                  为 None 时从 cookie 字符串重建（适用于 --cookie 粘贴路径）。
    """
    from macli.session import ConsoleSession

    cftk = _extract_cftk(ck)
    dprint("[green]✓ 从 cookie 中提取 cftk[/green]")

    # 先保存 cookie，登录凭证不依赖后续探测
    d = load_session(); d["cookie_str"] = ck; save_session(d)

    # 复用登录 session（有 console 上下文），或从 cookie 字符串重建
    if http_session is not None:
        http = http_session
    else:
        http = _new_session()
        for part in ck.split(";"):
            k, _, v = part.strip().partition("=")
            if k: http.cookies.set(k.strip(), v.strip())

    # 探测 region/project_id
    dprint("[cyan]获取账号区域信息...[/cyan]")
    me = _me_probe(http, cftk)
    support_regions = me.get("supportRegions", [])

    if not support_regions:
        # _me 探测失败：cookie 已保存，提示后续手动配置区域
        cprint("[yellow]无法自动获取区域信息，请登录后执行：[/yellow]")
        cprint("[dim]  macli region select    # 配置区域[/dim]")
        cprint("[dim]  macli workspace select  # 配置工作空间[/dim]")
        return

    if interactive:
        regions = sorted(r for r in support_regions if r in REGION_NAMES)
        cprint("\n[bold]可用区域：[/bold]")
        for i, r in enumerate(regions, 1):
            cprint(f"  [cyan]{i}.[/cyan] {r}  [dim]{REGION_NAMES[r]}[/dim]")
        while True:
            choice = input(f"\n请选择区域 (1-{len(regions)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(regions):
                region = regions[int(choice) - 1]; break
            cprint("[red]输入无效，请重试[/red]")
    else:
        region = me.get("region", "cn-north-9")
        dprint(f"[cyan]使用默认区域: {region}  {REGION_NAMES.get(region,'')}[/cyan]")

    # 获取该 region 的 project_id
    dprint(f"[cyan]获取 {region} 的 project_id...[/cyan]")
    me2        = _me(http, region, cftk)
    project_id = me2.get("projectId", "")
    agency_id  = me2.get("id") or me2.get("userId", "")
    if not project_id:
        cprint(f"[red]无法获取 {region} 的 project_id[/red]"); sys.exit(1)
    dprint(f"[green]✓ region={region}  project_id={project_id}[/green]")

    sess = ConsoleSession()
    sess.init(ck, region, project_id, agency_id, cftk, "")

    if interactive:
        wsid = _select_workspace(sess)
        sess.workspace_id = wsid
        d = load_session(); d["workspace_id"] = wsid; save_session(d)
    else:
        dprint("[cyan]获取工作空间列表...[/cyan]")
        workspaces = _fetch_workspaces(sess)
        if workspaces:
            wsid  = workspaces[0]["id"]
            wsname = workspaces[0]["name"]
            sess.workspace_id = wsid
            d = load_session(); d["workspace_id"] = wsid; save_session(d)
            cprint(f"[green]✓ 默认工作空间: {wsname}[/green]")
            dprint("[dim]提示：使用 workspace select 切换工作空间[/dim]")
        else:
            cprint("[yellow]未获取到工作空间，使用 workspace select 手动设置[/yellow]")


def _manual_cookie_input() -> str:
    """展示手动获取 cookie 指南，让用户粘贴，返回 cookie 字符串"""
    cprint("""
[bold cyan]══ 手动获取 cookie ══[/bold cyan]
[yellow]1.[/yellow] 浏览器访问 https://auth.huaweicloud.com/authui/login.html
[yellow]2.[/yellow] 完成登录（含短信验证码）
[yellow]3.[/yellow] 登录后访问 https://console.huaweicloud.com/modelarts/
[yellow]4.[/yellow] F12 → Network → 找到 [green]training-job-searches[/green] 的 POST 请求
[yellow]5.[/yellow] 复制 Request Headers 中的 [green]cookie[/green] 整行，粘贴后回车
""")
    cprint("[yellow]请粘贴 cookie：[/yellow]")
    sys.stdout.flush()
    return sys.stdin.readline().strip()
