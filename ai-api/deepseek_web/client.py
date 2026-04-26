import asyncio
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from playwright.async_api import BrowserContext, Page, async_playwright

logger = logging.getLogger(__name__)

DEEPSEEK_URL = "https://chat.deepseek.com/"

_SEND_SEL = (
    "button[type='submit'], "
    "button[aria-label*='send' i], "
    "button[aria-label*='发送'], "
    "button:has-text('Send'), "
    "button:has-text('发送')"
)

_STOP_SEL = (
    "button[aria-label*='stop' i], "
    "button[aria-label*='停止'], "
    "button:has-text('Stop'), "
    "button:has-text('停止')"
)

_INPUT_SELECTORS = [
    "textarea",
    "div[contenteditable='true'][role='textbox']",
    "div[contenteditable='true'][data-lexical-editor='true']",
    "div[contenteditable='true']",
]

_SKIP_DIRS = {
    "Cache",
    "Code Cache",
    "GPUCache",
    "DawnCache",
    "GrShaderCache",
    "ShaderCache",
    "blob_storage",
}

_TRANSIENT_MARKERS = [
    "思考中",
    "正在思考",
    "Searching",
    "Thinking",
]

_LOGIN_MARKERS = ["登录", "注册", "验证码", "sign in", "log in"]
_SIDEBAR_MARKERS = ["开启新对话", "今天", "7 天内", "30 天内", "用户问号回应与帮助"]


def _default_udata() -> str:
    return os.path.expandvars(r"C:\Users\%USERNAME%\AppData\Local\Google\Chrome\User Data")


def _load_config() -> tuple[str, str, int, bool, str]:
    try:
        from app.core.config import settings

        udata = (
            getattr(settings, "deepseek_chrome_user_data", "")
            or getattr(settings, "qwen_chrome_user_data", "")
            or getattr(settings, "chatgpt_chrome_user_data", "")
            or _default_udata()
        )
        profile = (
            getattr(settings, "deepseek_chrome_profile", "")
            or getattr(settings, "qwen_chrome_profile", "")
            or getattr(settings, "chatgpt_chrome_profile", "")
            or "Default"
        )
        max_conc = int(getattr(settings, "deepseek_chrome_max_concurrency", 0) or 5)
        force_no_proxy = bool(getattr(settings, "deepseek_chrome_force_no_proxy", True))
        service_udata = (getattr(settings, "deepseek_chrome_service_user_data", "") or "").strip()
    except Exception:
        udata, profile, max_conc, force_no_proxy, service_udata = _default_udata(), "Default", 5, True, ""
    return udata, profile, max_conc, force_no_proxy, service_udata


class DeepSeekWebClient:
    _instance: "DeepSeekWebClient | None" = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._pw = None
        self._context: BrowserContext | None = None
        self._tmp_dir: str | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._ready = False

    @classmethod
    async def get(cls) -> "DeepSeekWebClient":
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            inst = cls._instance
            if not inst._ready:
                await inst._init()
            return inst

    async def _init(self) -> None:
        user_data, profile, max_conc, force_no_proxy, service_udata = _load_config()
        self._semaphore = asyncio.Semaphore(max_conc)
        if service_udata:
            Path(service_udata).mkdir(parents=True, exist_ok=True)
            self._tmp_dir = None
            launch_user_data_dir = service_udata
            logger.info("deepseek_web | using persistent service user data: %s", service_udata)
        else:
            self._tmp_dir = _copy_profile(user_data, profile)
            launch_user_data_dir = self._tmp_dir

        self._pw = await async_playwright().start()
        launch_args = [
            f"--profile-directory={profile}",
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-notifications",
            "--mute-audio",
            "--window-position=9999,9999",
        ]
        # Must not use proxy for DeepSeek.
        if force_no_proxy:
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
        )

        page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await page.goto(DEEPSEEK_URL, wait_until="domcontentloaded", timeout=90_000)
        await asyncio.sleep(3)
        if await _auth_required(page):
            await self._teardown()
            raise RuntimeError(
                "DeepSeek session is not logged in. Please log in at https://chat.deepseek.com/ and retry."
            )

        self._ready = True
        logger.info("deepseek_web | client ready")

    async def analyze(self, prompt: str, timeout_ms: int = 120_000) -> dict[str, Any]:
        self._assert_ready()
        last_exc: Exception | None = None
        async with self._semaphore:
            for attempt in range(1, 3):
                page = await self._context.new_page()
                try:
                    return await _run_query(page, prompt, timeout_ms)
                except Exception as exc:
                    last_exc = exc
                    logger.warning("deepseek_web | analyze attempt %s failed: %s", attempt, exc)
                    await asyncio.sleep(1.2 * attempt)
                finally:
                    await page.close()
        raise RuntimeError(str(last_exc) if last_exc else "DeepSeek analyze failed")

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
                logger.warning("deepseek_web | batch failed code=%s err=%s", item.get("code"), exc)
                return {"code": item.get("code", ""), "name": item.get("name", ""), "error": str(exc)}

        return list(await asyncio.gather(*[_one(it) for it in items]))

    async def close(self) -> None:
        async with self.__class__._lock:
            await self._teardown()
            self.__class__._instance = None

    async def _teardown(self) -> None:
        self._ready = False
        for attr, closer in (("_context", "close"), ("_pw", "stop")):
            obj = getattr(self, attr, None)
            if obj:
                try:
                    await getattr(obj, closer)()
                except Exception:
                    pass
                setattr(self, attr, None)
        if self._tmp_dir:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None
        logger.info("deepseek_web | client closed")

    def _assert_ready(self) -> None:
        if not self._ready:
            raise RuntimeError("DeepSeekWebClient is not ready. Call `await DeepSeekWebClient.get()` first.")


async def shutdown() -> None:
    inst = DeepSeekWebClient._instance
    if inst and inst._ready:
        await inst.close()


def _copy_profile(user_data: str, profile: str) -> str:
    user_root = Path(user_data)
    src = user_root / profile
    tmp_root = Path(tempfile.mkdtemp(prefix="deepseek_svc_"))
    dst = tmp_root / profile

    def _ignore(_, contents):
        return {c for c in contents if c in _SKIP_DIRS}

    if src.exists():
        try:
            shutil.copytree(src, dst, ignore=_ignore, dirs_exist_ok=False)
            local_state = user_root / "Local State"
            if local_state.exists():
                shutil.copy2(local_state, tmp_root / "Local State")
        except Exception as exc:
            raise RuntimeError(
                f"Failed to copy Chrome profile '{profile}'. Close Chrome using this profile and retry."
            ) from exc
    else:
        dst.mkdir(parents=True, exist_ok=True)

    return str(tmp_root)


async def _auth_required(page: Page) -> bool:
    return await page.evaluate(
        """() => {
            const t = (document.body && document.body.innerText) ? document.body.innerText.toLowerCase() : '';
            const inSignIn = /\\/sign_in/.test(location.pathname.toLowerCase());
            return inSignIn || /log in|sign in|验证码|登录|注册/.test(t);
        }"""
    )


async def _find_input(page: Page):
    for _ in range(8):
        for sel in _INPUT_SELECTORS:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=1200):
                    return loc
            except Exception:
                pass
        await asyncio.sleep(0.6)
    return None


async def _fill_prompt(page: Page, prompt: str) -> None:
    target = await _find_input(page)
    if not target:
        raise RuntimeError("DeepSeek input box not found.")

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
    try:
        if await send.is_visible(timeout=3000):
            if await send.is_disabled():
                raise RuntimeError("DeepSeek send button is disabled.")
            await send.click()
            return
    except Exception:
        pass
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
                    '.ds-message .ds-markdown',
                    '.ds-message .ds-markdown-paragraph',
                    '.ds-message',
                    '[data-message-author-role="assistant"] .markdown, [data-message-author-role="assistant"]',
                    '[data-role="assistant"]',
                    '.assistant-message',
                    '.markdown-body',
                    '[class*="assistant"] [class*="markdown"]',
                    '[class*="message-content"]',
                ];
                for (const sel of selectors) {
                    const nodes = document.querySelectorAll(sel);
                    if (!nodes.length) continue;
                    let i = nodes.length - 1;
                    while (i >= 0) {
                        const node = nodes[i];
                        const cls = (node.className || '').toString();
                        // Skip obvious side/nav/list containers.
                        if (/sidebar|history|menu|nav/i.test(cls)) {
                            i -= 1;
                            continue;
                        }
                        const text = (node.innerText || '').trim();
                        if (text) return text;
                        i -= 1;
                    }
                }
                const bodyText = (document.body && document.body.innerText) ? document.body.innerText.trim() : '';
                return bodyText ? bodyText.slice(-3000) : '';
            }"""
        )
    ) or ""


def _invalid_response_reason(resp: str, prompt: str) -> str | None:
    t = (resp or "").strip()
    if not t:
        return "empty response"
    if t == prompt.strip():
        return "prompt echo"
    if sum(1 for m in _LOGIN_MARKERS if m.lower() in t.lower()) >= 2:
        return "landing/login page text"
    if sum(1 for m in _SIDEBAR_MARKERS if m in t) >= 2:
        return "sidebar/history text"
    if any(m in t for m in _TRANSIENT_MARKERS) and len(t) < 40:
        return "transient status"
    return None


async def _run_query(page: Page, prompt: str, timeout_ms: int) -> dict[str, Any]:
    await page.goto(DEEPSEEK_URL, wait_until="domcontentloaded", timeout=90_000)
    await asyncio.sleep(2.0)
    if await _auth_required(page):
        raise RuntimeError("DeepSeek requires login for chat. Please log in and retry.")

    await _fill_prompt(page, prompt)
    await asyncio.sleep(0.3)
    await _send_prompt(page)
    elapsed = await _wait_done(page, timeout_ms)

    t_deadline = time.time() + min(timeout_ms / 1000.0, 90.0)
    resp = await _extract_response(page)
    reason = _invalid_response_reason(resp, prompt)
    while reason == "transient status" and time.time() < t_deadline:
        await asyncio.sleep(1.2)
        resp = await _extract_response(page)
        reason = _invalid_response_reason(resp, prompt)
    if reason:
        raise RuntimeError(f"DeepSeek returned invalid output: {reason}")

    has_citation = bool(re.search(r"https?://|source|来源|引用", resp, re.I))
    return {"response": resp, "elapsed_s": round(elapsed, 2), "has_citation": has_citation}
