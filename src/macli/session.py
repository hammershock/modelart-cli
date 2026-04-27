"""ConsoleSession / API 封装、会话恢复"""
import os, sys, json, time, re, copy

import requests
from macli.constants import CONSOLE_BASE, SessionExpiredError, console
from macli.config import load_session, save_session, load_identityfiles
from macli.log import cprint, dprint
from macli.net import _new_session
from macli.helpers import enrich_ssh_entries


class ConsoleSession:

    def __init__(self):
        self.http         = _new_session()
        self.project_id   = None
        self.region       = None
        self.agency_id    = None
        self.workspace_id = None
        self.cftk         = None

    def init(self, cookie_str, region, project_id, agency_id, cftk, workspace_id=""):
        self.region       = region
        self.project_id   = project_id
        self.agency_id    = agency_id
        self.cftk         = cftk
        self.workspace_id = workspace_id
        for part in cookie_str.split(";"):
            k, _, v = part.strip().partition("=")
            if k: self.http.cookies.set(k.strip(), v.strip())
        self._set_headers()

        data = load_session()
        data.update({
            "region":       region,
            "project_id":   project_id,
            "agency_id":    agency_id,
            "workspace_id": workspace_id,
            "cftk":         cftk,
            "cookies":      {c.name: c.value for c in self.http.cookies},
            "cookie_str":   cookie_str,
            "saved_at":     time.time(),
        })
        save_session(data)

    def restore(self):
        d = load_session()
        if not d: return False
        self.region       = d.get("region", "cn-north-9")
        self.project_id   = d.get("project_id")
        self.agency_id    = d.get("agency_id", "")
        self.workspace_id = d.get("workspace_id", "")
        self.cftk         = d.get("cftk")
        for k, v in d.get("cookies", {}).items():
            self.http.cookies.set(k, v)
        self._set_headers()
        return bool(self.project_id)

    def _set_headers(self):
        h = {
            "accept":           "application/json, text/plain, */*",
            "content-type":     "application/json; charset=UTF-8",
            "region":           self.region,
            "projectname":      self.region,
            "agencyid":         self.agency_id,
            "x-language":       "zh-cn",
            "x-requested-with": "XMLHttpRequest",
            "x-target-services":"modelarts-iam5",
            "origin":           "https://console.huaweicloud.com",
            "user-agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "referer":          f"https://console.huaweicloud.com/modelarts/?locale=zh-cn"
                                f"&agencyId={self.agency_id}&region={self.region}",
        }
        if self.cftk:
            h["cftk"]     = self.cftk
            h["cf2-cftk"] = "cftk"
        self.http.headers.update(h)

    def check_login(self) -> bool:
        """调用 /modelarts/rest/me 验证当前 session 是否仍然有效。
        返回 True 表示有效，False 表示已过期或未登录。
        """
        try:
            r = self.http.get(
                "https://console.huaweicloud.com/modelarts/rest/me",
                headers={
                    "accept":           "application/json, text/plain, */*",
                    "region":           self.region or "",
                    "projectname":      self.region or "",
                    "agencyid":         self.agency_id or "",
                    "x-language":       "zh-cn",
                    "x-requested-with": "XMLHttpRequest",
                    "x-target-services":"modelarts-iam5",
                    "cftk":             self.cftk or "",
                    "cf2-cftk":         "cftk",
                },
                timeout=10,
            )
            if r.status_code != 200:
                return False
            data = r.json()
            # projectId 存在且非空说明 session 有效
            return bool(data.get("projectId"))
        except Exception:
            return False

    @property
    def base(self):
        return f"{CONSOLE_BASE}/modelarts/rest/trainingJob/v2/{self.project_id}"

    def _checked_request(self, method: str, url: str, **kwargs):
        """执行 HTTP 请求，遇到网络/SSL 异常时先检查登录状态：
        - session 已过期 → 抛 SessionExpiredError（exit 2，触发自动重登录）
        - 纯网络问题    → 抛原始异常
        """
        try:
            return getattr(self.http, method)(url, **kwargs)
        except requests.exceptions.SSLError as e:
            if not self.check_login():
                raise SessionExpiredError("session expired, please login again") from e
            raise
        except requests.exceptions.ConnectionError as e:
            if not self.check_login():
                raise SessionExpiredError("session expired, please login again") from e
            raise

    def get(self, path, **params):
        return self._checked_request(
            "get", f"{self.base}{path}",
            params=params or None, timeout=20,
        )

    def post(self, path, body):
        return self._checked_request(
            "post", f"{self.base}{path}",
            json=body, timeout=30,
        )

# ── API ──────────────────────────────────────────────────────

class API:
    def __init__(self, sess: ConsoleSession):
        self.sess = sess

    def _safe_json(self, r):
        """安全解析响应 JSON。
        若解析失败（如响应体为空），自动检查登录状态：
        - 登录已过期 → 抛出 SessionExpiredError
        - 其他原因   → 抛出原始 JSONDecodeError
        """
        try:
            return r.json()
        except requests.exceptions.JSONDecodeError as e:
            # 响应体为空或非 JSON，检查登录状态
            if not self.sess.check_login():
                raise SessionExpiredError(
                    "登录凭据已过期，请重新执行 macli login"
                ) from e
            raise

    def list_jobs(self, limit=50, offset=0, order="desc") -> dict:
        r = self.sess.post("/training-job-searches", {
            "workspace_id": self.sess.workspace_id,
            "limit":        limit,
            "offset":       offset,
            "order":        order,
            "sort_by":      "create_time",
        })
        dprint(f"[dim]API list_jobs offset={offset} limit={limit} order={order} → {r.status_code}[/dim]")
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]列表失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job(self, job_id: str) -> dict:
        r = self.sess.get(f"/training-jobs/{job_id}")
        dprint(f"[dim]API get_job {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]详情失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job_events(self, job_id: str, limit: int = 50, offset: int = 0,
                       start_time=None, end_time=None, order: str = "desc",
                       pattern: str = "", level: str = "") -> dict:
        params = {
            "limit": limit,
            "offset": offset,
            "order": order,
            "pattern": pattern,
            "level": level,
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        r = self.sess.get(f"/training-jobs/{job_id}/events", **params)
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]事件查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_job_tasks(self, job_id: str) -> list:
        r = self.sess.get(f"/training-jobs/{job_id}/tasks")
        dprint(f"[dim]API get_job_tasks {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code == 200:
            data = self._safe_json(r)
            tasks = data if isinstance(data, list) else []
            dprint(f"[dim]  tasks: {[t.get('name','?') for t in tasks]}[/dim]")
            return tasks
        cprint(f"[red]任务列表查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return []

    def get_job_log_url(self, job_id: str, task_id: str, content_type: str = "application/octet-stream") -> dict:
        url = f"{self.sess.base}/training-jobs/{job_id}/tasks/{task_id}/logs/url"
        r = self.sess.http.get(url, params={"Content-Type": content_type}, timeout=30)
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]日志下载链接获取失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def download_from_obs_url(self, obs_url: str, timeout: int = 120):
        http = _new_session()
        return http.get(
            obs_url,
            headers={
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "origin": "https://console.huaweicloud.com",
                "referer": "https://console.huaweicloud.com/",
                "user-agent": self.sess.http.headers.get("user-agent", "Mozilla/5.0"),
            },
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )

    def query_usage_range(self, query: str, start: int, end: int, step: int = 60) -> dict:
        url = (
            f"https://console.huaweicloud.com/modelarts/rest/api/aompod/v1/"
            f"{self.sess.project_id}/aom/api/v1/query_range"
        )
        payload = {"query": query, "start": start, "end": end, "step": step}
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://console.huaweicloud.com",
            "referer": "https://console.huaweicloud.com/",
            "user-agent": "Mozilla/5.0",
            "region": self.sess.region,
            "projectname": self.sess.region,
            "agencyid": self.sess.agency_id,
            "cftk": self.sess.cftk,
            "cf2-cftk": "cftk",
        }
        http = _new_session()
        for c in self.sess.http.cookies:
            http.cookies.set(c.name, c.value)
        r = http.post(url, params=payload, json=payload, headers=headers, timeout=30)
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]usage 查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    def get_exec_status(self, job_id: str) -> dict:
        r = self.sess.get(f"/training-jobs/{job_id}/exec/status")
        if r.status_code == 200:
            return self._safe_json(r)
        cprint(f"[red]CloudShell 状态查询失败 {r.status_code}: {r.text[:200]}[/red]")
        return {}

    # flavor_id 对应卡数
    FLAVOR_MAP = {
        1: "modelarts.pool.visual.xlarge",
        2: "modelarts.pool.visual.2xlarge",
        4: "modelarts.pool.visual.4xlarge",
        8: "modelarts.pool.visual.8xlarge",
    }

    def copy_job(self, job_id: str,
                 new_gpu_count: int = None,
                 new_name: str = None,
                 description: str = None,
                 command: str = None) -> dict:
        src = self.get_job(job_id)
        if not src:
            return {}

        body = copy.deepcopy(src)

        # 清理只读字段
        meta = body.get("metadata", {})
        for k in ("id", "uuid", "create_time", "update_time",
                  "training_experiment_reference", "tags"):
            meta.pop(k, None)

        # 作业名称
        if new_name:
            meta["name"] = new_name
        else:
            clean = re.sub(r'-copy-\d+$', '', meta.get("name", "job"))
            meta["name"] = f"{clean}-copy-{int(time.time()) % 100000}"

        # 描述
        if description is not None:
            meta["description"] = description

        body["metadata"] = meta

        # 清理服务端只读字段
        body.pop("status", None)
        body.pop("ftjob_config", None)
        # endpoints 中只保留 key_pair_names（密钥对配置），清掉 task_urls（运行时动态填充）
        endpoints = body.get("endpoints", {})
        ssh = endpoints.get("ssh", {})
        if ssh:
            endpoints["ssh"] = {"key_pair_names": ssh.get("key_pair_names", [])}
            body["endpoints"] = endpoints
        else:
            body.pop("endpoints", None)
        res = body.get("spec", {}).get("resource", {})
        res.pop("pool_info", None)
        res.pop("main_container_allocated_resources", None)

        # 修改规格（只改 flavor_id）
        if new_gpu_count is not None:
            flavor = self.FLAVOR_MAP.get(new_gpu_count)
            if not flavor:
                cprint(f"[red]不支持的卡数 {new_gpu_count}，可选: 1/2/4/8[/red]")
                return {}
            body["spec"]["resource"]["flavor_id"] = flavor

        # 修改启动命令
        if command is not None:
            body.setdefault("algorithm", {})["command"] = command

        r = self.sess.post("/training-jobs", body)
        dprint(f"[dim]API copy_job → {r.status_code}[/dim]")
        if r.status_code in (200, 201):
            created = self._safe_json(r)
            dprint(f"[dim]  新作业 ID={created.get('metadata',{}).get('id','?')} phase={created.get('status',{}).get('phase','?')}[/dim]")
            return created
        dprint(f"[dim]  error body: {r.text[:400]}[/dim]")
        cprint(f"[red]创建失败 {r.status_code}: {r.text[:400]}[/red]")
        return {}

    def delete_job(self, job_id: str) -> bool:
        r = self.sess.http.delete(
            f"{self.sess.base}/training-jobs/{job_id}",
            json={}, timeout=15)
        dprint(f"[dim]API delete_job {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code in (200, 202):
            return True
        cprint(f"[red]删除失败 {r.status_code}: {r.text[:200]}[/red]")
        return False

    def stop_job(self, job_id: str) -> bool:
        r = self.sess.post(
            f"/training-jobs/{job_id}/actions",
            {"action_type": "terminate"})
        dprint(f"[dim]API stop_job {job_id[:8]}… → {r.status_code}[/dim]")
        if r.status_code in (200, 202):
            return True
        cprint(f"[red]终止失败 {r.status_code}: {r.text[:200]}[/red]")
        return False

    def get_ssh(self, job: dict) -> list:
        """从详情中提取 SSH 信息，路径: endpoints.ssh.task_urls[].url，并补充 port。"""
        try:
            return enrich_ssh_entries(job.get("endpoints", {}).get("ssh", {}).get("task_urls", []))
        except Exception:
            return []

# ── 命令 ─────────────────────────────────────────────────────

def _sess_or_exit():
    sess = ConsoleSession()
    if not sess.restore():
        cprint("[red]未找到 session，请先执行 login[/red]")
        sys.exit(1)
    return sess
