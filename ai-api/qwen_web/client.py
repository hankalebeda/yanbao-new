import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

logger = logging.getLogger(__name__)

QWEN_URL = "https://chat.qwen.ai/"

_STOP_SEL = (
    "button[aria-label*='stop' i], "
    "button:has-text('Stop')"
)

_SEND_SEL = (
    "button.send-button, "
    "button[type='submit'], "
    "button[aria-label*='send' i], "
    "button:has-text('Send'), "
    "[data-testid*='send' i]"
)

_INPUT_SELECTORS = [
    "textarea.message-input-textarea",
    "textarea",
    "div[contenteditable='true'][role='textbox']",
    "div[contenteditable='true'][data-lexical-editor='true']",
    "div[contenteditable='true'][aria-label*='message' i]",
    "div[contenteditable='true']",
]

# Keep IndexedDB to preserve Qwen auth state.
_SKIP_DIRS = {
    "Cache",
    "Code Cache",
    "GPUCache",
    "DawnCache",
    "GrShaderCache",
    "ShaderCache",
    "blob_storage",
}


LANDING_NOISE_MARKERS = [
    "使用 Qwen Chat",
    "人工智能生成的内容可能不准确",
    "登录",
    "注册",
    "你想知道什么",
    "用户条款",
    "隐私协议",
]

TRANSIENT_MARKERS = [
    "正在搜索网络",
    "正在提炼核心信息",
    "精炼信息以准确回应",
    "正在思考",
    "已完成思考",
]


def _default_udata() -> str:
    return os.path.expandvars(r"C:\Users\%USERNAME%\AppData\Local\Google\Chrome\User Data")


def _load_config() -> tuple[str, str, int, str, bool, str, bool, str]:
    try:
        from app.core.config import settings

        udata = (
            getattr(settings, "qwen_chrome_user_data", "")
            or getattr(settings, "chatgpt_chrome_user_data", "")
            or getattr(settings, "gemini_chrome_user_data", "")
            or _default_udata()
        )
        profile = (
            getattr(settings, "qwen_chrome_profile", "")
            or getattr(settings, "chatgpt_chrome_profile", "")
            or getattr(settings, "gemini_chrome_profile", "")
            or "Default"
        )
        max_conc = int(getattr(settings, "qwen_max_concurrency", 0) or 5)
        proxy_url = getattr(settings, "qwen_proxy_url", "") or ""
        force_no_proxy = bool(getattr(settings, "qwen_force_no_proxy", True))
        cdp_url = getattr(settings, "qwen_cdp_url", "") or ""
        cdp_hide_window = bool(getattr(settings, "qwen_cdp_hide_window", True))
        service_udata = (getattr(settings, "qwen_service_user_data", "") or "").strip()
    except Exception:
        udata, profile, max_conc, proxy_url, force_no_proxy, cdp_url, cdp_hide_window, service_udata = (
            _default_udata(),
            "Default",
            5,
            "",
            True,
            "",
            True,
            "",
        )
    return udata, profile, max_conc, proxy_url, force_no_proxy, cdp_url, cdp_hide_window, service_udata


class QwenWebClient:
    _instance: "QwenWebClient | None" = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._pw = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._tmp_dir: str | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._ready = False
        self._attached_cdp = False
        self._cdp_hide_window = True
        self._windows_hidden = False

    @classmethod
    async def get(cls) -> "QwenWebClient":
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            inst = cls._instance
            if not inst._ready:
                await inst._init()
            return inst

    async def _init(self) -> None:
        user_data, profile, max_conc, proxy_url, force_no_proxy, cdp_url, cdp_hide_window, service_udata = _load_config()
        self._semaphore = asyncio.Semaphore(max_conc)
        self._cdp_hide_window = cdp_hide_window
        self._pw = await async_playwright().start()
        if cdp_url:
            resolved_cdp = _resolve_cdp_ws_url(cdp_url)
            self._browser = await self._pw.chromium.connect_over_cdp(resolved_cdp, timeout=30_000)
            self._attached_cdp = True
            if self._browser.contexts:
                self._context = self._browser.contexts[0]
            else:
                self._context = await self._browser.new_context(viewport={"width": 1280, "height": 900})
            logger.info("qwen_web | attached to existing Chrome via CDP: %s", resolved_cdp)
        else:
            if service_udata:
                Path(service_udata).mkdir(parents=True, exist_ok=True)
                self._tmp_dir = None
                launch_user_data_dir = service_udata
                logger.info("qwen_web | using persistent service user data: %s", service_udata)
            else:
                self._tmp_dir = _copy_profile(user_data, profile)
                launch_user_data_dir = self._tmp_dir
            launch_kwargs: dict[str, Any] = {}
            launch_args = [
                f"--profile-directory={profile}",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-notifications",
                "--mute-audio",
                "--window-position=9999,9999",
            ]
            if proxy_url:
                launch_kwargs["proxy"] = {"server": proxy_url}
            elif force_no_proxy:
                launch_args.extend(
                    [
                        "--no-proxy-server",
                        "--proxy-server=direct://",
                        "--proxy-bypass-list=*",
                    ]
                )

            self._context = await self._pw.chromium.launch_persistent_context(
                user_data_dir=launch_user_data_dir,
                channel="chrome",
                headless=False,
                args=launch_args,
                viewport={"width": 1280, "height": 900},
                **launch_kwargs,
            )

        page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await page.goto(QWEN_URL, wait_until="domcontentloaded", timeout=90_000)
        await asyncio.sleep(4)
        logger.info("qwen_web | preflight url=%s", page.url)

        if await _auth_required(page):
            await self._teardown()
            raise RuntimeError(
                "Qwen session is not logged in. Log in at https://chat.qwen.ai/ with the configured Chrome profile."
            )

        self._ready = True
        logger.info("qwen_web | client ready")

    async def analyze(self, prompt: str, timeout_ms: int = 120_000) -> dict[str, Any]:
        self._assert_ready()
        last_exc: Exception | None = None
        async with self._semaphore:
            for attempt in range(1, 3):
                page = await self._context.new_page()
                try:
                    result = await _run_query(page, prompt, timeout_ms)
                    if self._attached_cdp and self._cdp_hide_window and not self._windows_hidden:
                        await self._hide_cdp_windows(page)
                        self._windows_hidden = True
                    return result
                except Exception as exc:
                    last_exc = exc
                    logger.warning("qwen_web | analyze attempt %s failed: %s", attempt, exc)
                    await asyncio.sleep(1.2 * attempt)
                finally:
                    await page.close()
        raise RuntimeError(str(last_exc) if last_exc else "Qwen analyze failed")

    async def analyze_batch(
        self,
        items: list[dict[str, str]],
        timeout_ms: int = 120_000,
    ) -> list[dict[str, Any]]:
        self._assert_ready()

        async def _one(item: dict[str, str]) -> dict[str, Any]:
            try:
                res = await self.analyze(item["prompt"], timeout_ms=timeout_ms)
                return {"code": item.get("code", ""), "name": item.get("name", ""), **res}
            except Exception as exc:
                logger.warning("qwen_web | batch failed code=%s err=%s", item.get("code"), exc)
                return {"code": item.get("code", ""), "name": item.get("name", ""), "error": str(exc)}

        return list(await asyncio.gather(*[_one(it) for it in items]))

    async def close(self) -> None:
        async with self.__class__._lock:
            await self._teardown()
            self.__class__._instance = None

    async def _teardown(self) -> None:
        self._ready = False
        if not self._attached_cdp:
            obj = self._context
            if obj:
                try:
                    await obj.close()
                except Exception:
                    pass
        self._context = None
        self._browser = None
        if self._pw:
            try:
                await self._pw.stop()
            except Exception:
                pass
            self._pw = None
        if self._tmp_dir:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None
        self._attached_cdp = False
        self._windows_hidden = False
        logger.info("qwen_web | client closed")

    def _assert_ready(self) -> None:
        if not self._ready:
            raise RuntimeError("QwenWebClient is not ready. Call `await QwenWebClient.get()` first.")

    async def _hide_cdp_windows(self, page: Page) -> None:
        try:
            session = await page.context.new_cdp_session(page)
            info = await session.send("Browser.getWindowForTarget")
            window_id = info.get("windowId")
            if window_id is None:
                return
            await session.send(
                "Browser.setWindowBounds",
                {
                    "windowId": window_id,
                    "bounds": {
                        "left": 32000,
                        "top": 32000,
                        "width": 1280,
                        "height": 900,
                        "windowState": "normal",
                    },
                },
            )
        except Exception as exc:
            logger.debug("qwen_web | hide window skipped: %s", exc)


async def shutdown() -> None:
    inst = QwenWebClient._instance
    if inst and inst._ready:
        await inst.close()


def _resolve_cdp_ws_url(cdp_url: str) -> str:
    u = (cdp_url or "").strip()
    if u.startswith("ws://") or u.startswith("wss://"):
        return u
    if u.startswith("http://") or u.startswith("https://"):
        base = u.rstrip("/")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f"{base}/json/version", timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        ws_url = (data or {}).get("webSocketDebuggerUrl", "").strip()
        if not ws_url:
            raise RuntimeError(f"CDP endpoint did not provide webSocketDebuggerUrl: {u}")
        return ws_url
    raise RuntimeError(f"Invalid qwen_cdp_url: {cdp_url}")


def _copy_profile(user_data: str, profile: str) -> str:
    user_root = Path(user_data)
    src = user_root / profile
    tmp_root = Path(tempfile.mkdtemp(prefix="qwen_svc_"))
    dst = tmp_root / profile

    def _ignore(_, contents):
        return {c for c in contents if c in _SKIP_DIRS}

    if src.exists():
        try:
            shutil.copytree(src, dst, ignore=_ignore, dirs_exist_ok=False)
            # Chrome stores cookie encryption key in "Local State" at user data root.
            # Without this file, copied profile cookies may become unreadable.
            local_state = user_root / "Local State"
            if local_state.exists():
                shutil.copy2(local_state, tmp_root / "Local State")
        except Exception as exc:
            raise RuntimeError(
                f"Failed to copy Chrome profile '{profile}'. "
                "Close all Chrome windows using this profile and retry."
            ) from exc
    else:
        dst.mkdir(parents=True, exist_ok=True)

    return str(tmp_root)


async def _auth_required(page: Page) -> bool:
    return await page.evaluate(
        """() => {
            const txt = (document.body && document.body.innerText) ? document.body.innerText : '';
            const hasAuthBtns = !!document.querySelector('button.header-right-auth-button');
            const hasLoginWords = /登录|注册|login|sign up/i.test(txt);
            const hasWelcome = /欢迎|保持注销状态|welcome/i.test(txt);
            return hasAuthBtns && hasLoginWords && hasWelcome;
        }"""
    )


async def _dismiss_guidance(page: Page) -> None:
    for sel in ["button.guidance-pc-close-btn", "button:has-text('Close')"]:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=600):
                await loc.click()
                await asyncio.sleep(0.4)
        except Exception:
            pass


async def _enable_web_search(page: Page) -> bool:
    for sel in [
        "button[aria-label*='search' i]",
        "button:has-text('Search')",
        "button:has-text('联网')",
        "button:has-text('搜索')",
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                if await btn.get_attribute("aria-pressed") != "true":
                    await btn.click()
                    await asyncio.sleep(0.3)
                return True
        except Exception:
            pass
    return False


async def _find_input(page: Page):
    for _ in range(6):
        for sel in _INPUT_SELECTORS:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=1200):
                    return loc
            except Exception:
                pass
        await asyncio.sleep(0.5)
    return None


async def _fill_prompt(page: Page, prompt: str) -> None:
    target = await _find_input(page)
    if not target:
        raise RuntimeError("Qwen input box not found.")

    await target.click()
    tag = (await target.evaluate("e => e.tagName.toLowerCase()")).strip()
    if tag == "textarea":
        await target.fill(prompt)
        return

    ok = False
    try:
        ok = await target.evaluate(
            """(el, text) => {
                el.focus();
                const sel = window.getSelection();
                const range = document.createRange();
                range.selectNodeContents(el);
                sel.removeAllRanges();
                sel.addRange(range);
                const inserted = document.execCommand('insertText', false, text);
                el.dispatchEvent(new InputEvent('input', { bubbles: true }));
                return inserted;
            }""",
            prompt,
        )
    except Exception:
        ok = False

    if not ok:
        await page.keyboard.type(prompt)


async def _send_prompt(page: Page) -> None:
    send = page.locator(_SEND_SEL).first
    if await send.is_visible(timeout=3000):
        if await send.is_disabled():
            raise RuntimeError("Qwen send button is disabled.")
        await send.click()
        return
    await page.keyboard.press("Enter")


async def _wait_done(page: Page, timeout_ms: int) -> float:
    t0 = time.time()
    try:
        await page.wait_for_selector(_STOP_SEL, timeout=8_000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(_STOP_SEL, state="hidden", timeout=timeout_ms)
    except Exception:
        pass
    await asyncio.sleep(1.0)
    return time.time() - t0


async def _extract_response(page: Page) -> str:
    return (
        await page.evaluate(
            """() => {
                const selectors = [
                    '.qwen-chat-message:not(.qwen-chat-message-user)',
                    '.qwen-chat-message:not(.qwen-chat-message-user) .message-content',
                    '.qwen-chat-message:not(.qwen-chat-message-user) .markdown',
                    '[data-message-author-role="assistant"] .markdown, [data-message-author-role="assistant"]',
                    '[data-role="assistant"]',
                    '.assistant-message',
                    '.markdown-body',
                    '[class*="assistant"] [class*="markdown"]',
                    '.chat-user-message + .qwen-chat-message',
                ];
                for (const sel of selectors) {
                    const nodes = document.querySelectorAll(sel);
                    if (!nodes.length) continue;
                    let i = nodes.length - 1;
                    while (i >= 0) {
                        const node = nodes[i];
                        const cls = (node.className || '').toString();
                        if (cls.includes('qwen-chat-message-user') || cls.includes('chat-user-message')) {
                            i -= 1;
                            continue;
                        }
                        const text = (node.innerText || '').trim();
                        if (text) return text;
                        i -= 1;
                    }
                }
                const bodyText = (document.body && document.body.innerText) ? document.body.innerText.trim() : '';
                return bodyText ? bodyText.slice(-2500) : '';
            }"""
        )
    ) or ""


def _invalid_response_reason(resp: str, prompt: str) -> str | None:
    text = (resp or "").strip()
    if not text:
        return "empty response"
    if any(m in text for m in TRANSIENT_MARKERS) and len(text) < 40:
        return "transient status"
    if "跳过" in text and len(text) < 40:
        return "transient status"
    if text == prompt.strip():
        return "prompt echo"
    if sum(1 for m in LANDING_NOISE_MARKERS if m in text) >= 2:
        return "landing/login page text"
    return None


async def _run_query(page: Page, prompt: str, timeout_ms: int) -> dict[str, Any]:
    failed_requests: list[tuple[str, str]] = []

    def _on_failed(req):
        failure = req.failure or "unknown_error"
        failed_requests.append((req.url, str(failure)))

    page.on("requestfailed", _on_failed)

    load_ok = False
    for _ in range(3):
        await page.goto(QWEN_URL, wait_until="domcontentloaded", timeout=90_000)
        await asyncio.sleep(2.0)
        root_children = await page.evaluate("(document.getElementById('root') || {}).childElementCount || 0")
        if root_children > 0:
            load_ok = True
            break
        await asyncio.sleep(1.0)

    if not load_ok:
        blocked = [u for u, _ in failed_requests if "assets.alicdn.com" in u][:3]
        blocked_hint = f" blocked_urls={blocked}" if blocked else ""
        raise RuntimeError(
            "Qwen web app failed to load frontend assets. Check access to assets.alicdn.com"
            " or configure qwen_proxy_url." + blocked_hint
        )

    await _dismiss_guidance(page)

    if await _auth_required(page):
        raise RuntimeError("Qwen requires login for chat. Log in first and retry.")

    _ = await _enable_web_search(page)
    await _fill_prompt(page, prompt)
    await asyncio.sleep(0.2)
    await _send_prompt(page)
    elapsed = await _wait_done(page, timeout_ms)

    if await _auth_required(page):
        raise RuntimeError("Qwen requires login for chat. Log in first and retry.")

    t_deadline = time.time() + min(timeout_ms / 1000.0, 90.0)
    resp = await _extract_response(page)
    reason = _invalid_response_reason(resp, prompt)
    while reason == "transient status" and time.time() < t_deadline:
        await asyncio.sleep(1.2)
        resp = await _extract_response(page)
        reason = _invalid_response_reason(resp, prompt)
    if reason:
        raise RuntimeError(f"Qwen returned invalid output: {reason}")

    has_citation = bool(re.search(r"https?://|source|来源|引用", resp, re.I))
    return {"response": resp, "elapsed_s": round(elapsed, 2), "has_citation": has_citation}
