"""HTTP session 工厂 & 依赖自动安装"""
import os, sys, subprocess as _subprocess

import requests


def _ensure_pkg(*packages: str) -> None:
    """检查包是否已安装，缺少则自动 pip install 一次，然后重启脚本。"""
    import importlib
    missing = []
    for pkg in packages:
        import_name = {"Pillow": "PIL", "scikit-learn": "sklearn"}.get(pkg, pkg)
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(pkg)
    if not missing:
        return
    if os.environ.get("_MACLI_INSTALLED"):
        print(f"[macli] 错误：依赖 {', '.join(missing)} 安装后仍无法导入。")
        print(f"[macli] 请手动执行：pip install {' '.join(missing)}")
        sys.exit(1)
    print(f"[macli] 缺少依赖：{', '.join(missing)}，正在安装...")
    try:
        _subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", *missing]
        )
    except _subprocess.CalledProcessError:
        print(f"[macli] 安装失败，请手动执行：pip install {' '.join(missing)}")
        sys.exit(1)
    print("[macli] 安装完成，重新启动...")
    env = os.environ.copy()
    env["_MACLI_INSTALLED"] = "1"
    os.execve(sys.executable, [sys.executable] + sys.argv, env)


def _new_session() -> requests.Session:
    s = requests.Session()
    s.proxies = {"http": "", "https": "", "no_proxy": "*"}
    s.trust_env = False
    return s
