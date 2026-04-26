from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SERVICE_PROFILE_ROOT = ROOT / "runtime" / "web_login_profiles"


def _resolve_cfg() -> dict[str, dict[str, object]]:
    SERVICE_PROFILE_ROOT.mkdir(parents=True, exist_ok=True)
    deepseek_udata = SERVICE_PROFILE_ROOT / "deepseek"
    qwen_udata = SERVICE_PROFILE_ROOT / "qwen"
    deepseek_udata.mkdir(parents=True, exist_ok=True)
    qwen_udata.mkdir(parents=True, exist_ok=True)
    return {
        "deepseek": {
            "url": "https://chat.deepseek.com/",
            "user_data": str(deepseek_udata),
            "profile": "Default",
            "force_no_proxy": True,
        },
        "qwen": {
            "url": "https://chat.qwen.ai/",
            "user_data": str(qwen_udata),
            "profile": "Default",
            "force_no_proxy": True,
        },
    }


def _ensure_server(base_url: str) -> None:
    try:
        resp = httpx.get(f"{base_url.rstrip('/')}/health", timeout=5.0)
    except Exception as exc:
        raise RuntimeError(f"server not reachable: {exc}") from exc
    if resp.status_code != 200:
        raise RuntimeError(f"server health failed: status={resp.status_code}")


def _open_login_window(provider: str, cfg: dict[str, object]) -> None:
    asyncio.run(_open_login_window_async(provider, cfg))


async def _open_login_window_async(provider: str, cfg: dict[str, object]) -> None:
    url = str(cfg["url"])
    user_data = str(cfg["user_data"])
    profile = str(cfg["profile"])
    force_no_proxy = bool(cfg["force_no_proxy"])

    print(f"\n[{provider}] opening browser for manual login")
    print(f"[{provider}] url={url}")
    print(f"[{provider}] profile={user_data} / {profile}")
    print(f"[{provider}] login complete后，请直接关闭这个浏览器窗口。")

    launch_args = [
        f"--profile-directory={profile}",
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-notifications",
        "--mute-audio",
    ]
    if force_no_proxy:
        launch_args.extend(
            [
                "--no-proxy-server",
                "--proxy-server=direct://",
                "--proxy-bypass-list=*",
            ]
        )

    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=user_data,
            channel="chrome",
            headless=False,
            args=launch_args,
            viewport={"width": 1280, "height": 900},
        )
        closed = asyncio.Event()
        context.on("close", lambda: closed.set())
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        start = time.time()
        await closed.wait()
        print(f"[{provider}] browser closed after {round(time.time() - start, 1)}s")


def _run_real_test(
    base_url: str,
    timeout_s: int,
    inter_call_sleep_s: float,
    retry: int,
    providers: list[str],
    deepseek_user_data: str,
    qwen_user_data: str,
) -> int:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "test_webai_real.py"),
        "--base-url",
        base_url,
        "--providers",
        ",".join(providers),
        "--calls-per-provider",
        "1",
        "--timeout-s",
        str(timeout_s),
        "--retry",
        str(retry),
        "--inter-call-sleep-s",
        str(inter_call_sleep_s),
        "--inter-retry-sleep-s",
        "10",
    ]
    print("\n[step] running real smoke test for deepseek,qwen ...")
    env = os.environ.copy()
    env["DEEPSEEK_CHROME_SERVICE_USER_DATA"] = deepseek_user_data
    env["DEEPSEEK_CHROME_USER_DATA"] = deepseek_user_data
    env["DEEPSEEK_CHROME_PROFILE"] = "Default"
    env["QWEN_SERVICE_USER_DATA"] = qwen_user_data
    env["QWEN_CHROME_USER_DATA"] = qwen_user_data
    env["QWEN_CHROME_PROFILE"] = "Default"
    env["DEEPSEEK_CHROME_FORCE_NO_PROXY"] = "true"
    env["QWEN_FORCE_NO_PROXY"] = "true"
    return subprocess.run(cmd, cwd=str(ROOT), env=env, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual login (DeepSeek/Qwen) then auto real test.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--providers", default="deepseek,qwen")
    parser.add_argument("--timeout-s", type=int, default=120)
    parser.add_argument("--inter-call-sleep-s", type=float, default=6.0)
    parser.add_argument("--retry", type=int, default=1)
    args = parser.parse_args()

    _ensure_server(args.base_url)
    cfg = _resolve_cfg()
    providers = [p.strip() for p in args.providers.split(",") if p.strip() in {"deepseek", "qwen"}]
    if not providers:
        raise RuntimeError("providers is empty; use deepseek and/or qwen")

    if "deepseek" in providers:
        print("[step] deepseek manual login")
        _open_login_window("deepseek", cfg["deepseek"])

    if "qwen" in providers:
        print("[step] qwen manual login")
        _open_login_window("qwen", cfg["qwen"])

    return _run_real_test(
        base_url=args.base_url,
        timeout_s=args.timeout_s,
        inter_call_sleep_s=args.inter_call_sleep_s,
        retry=args.retry,
        providers=providers,
        deepseek_user_data=str(cfg["deepseek"]["user_data"]),
        qwen_user_data=str(cfg["qwen"]["user_data"]),
    )


if __name__ == "__main__":
    raise SystemExit(main())
