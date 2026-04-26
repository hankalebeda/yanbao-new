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

CHATGPT_URL = "https://chatgpt.com/"

_STOP_SEL = (
    "button[data-testid='stop-button'], "
    "button[aria-label*='Stop generating'], "
    "button[aria-label*='停止生成']"
)

_SEND_SEL = (
    "button[data-testid='send-button'], "
    "button[aria-label*='Send' i], "
    "button[aria-label*='发送']"
)

_SKIP_DIRS = {
    "Cache",
    "Code Cache",
    "GPUCache",
    "DawnCache",
    "GrShaderCache",
    "ShaderCache",
    "blob_storage",
    "Service Worker",
    "CacheStorage",
    "IndexedDB",
}

_MODEL_PROMPT = (
    "你当前底层主模型版本是什么？"
    "仅输出一个短字符串，格式必须是 GPT-X（例如 GPT-5 或 GPT-5.2）。"
    "不要输出任何解释、标点、额外文本。"
)


def _default_udata() -> str:
    return os.path.expandvars(r"C:\Users\%USERNAME%\AppData\Local\Google\Chrome\User Data")


def _load_config() -> tuple[str, str, int, bool]:
    try:
        from app.core.config import settings

        udata = (
            getattr(settings, "chatgpt_chrome_user_data", "")
            or getattr(settings, "gemini_chrome_user_data", "")
            or _default_udata()
        )
        profile = (
            getattr(settings, "chatgpt_chrome_profile", "")
            or getattr(settings, "gemini_chrome_profile", "")
            or "Default"
        )
        max_conc = int(getattr(settings, "chatgpt_max_concurrency", 0) or 5)
        require_5x = bool(getattr(settings, "chatgpt_require_5x", True))
    except Exception:
        udata, profile, max_conc, require_5x = _default_udata(), "Default", 5, True
    return udata, profile, max_conc, require_5x


class ChatGPTWebClient:
    _instance: "ChatGPTWebClient | None" = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._pw = None
        self._context: BrowserContext | None = None
        self._tmp_dir: str | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._ready = False
        self._model_probe: dict[str, Any] | None = None

    @property
    def model_probe(self) -> dict[str, Any] | None:
        return dict(self._model_probe) if self._model_probe else None

    @classmethod
    async def get(cls) -> "ChatGPTWebClient":
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            inst = cls._instance
            if not inst._ready:
                await inst._init()
            return inst

    async def _init(self) -> None:
        user_data, profile, max_conc, require_5x = _load_config()
        self._semaphore = asyncio.Semaphore(max_conc)

        logger.info("chatgpt_web | copying Chrome profile %s/%s", user_data, profile)
        self._tmp_dir = _copy_profile(user_data, profile)

        logger.info("chatgpt_web | launching Chrome")
        self._pw = await async_playwright().start()
        self._context = await self._pw.chromium.launch_persistent_context(
            user_data_dir=self._tmp_dir,
            channel="chrome",
            headless=False,
            args=[
                f"--profile-directory={profile}",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-notifications",
                "--mute-audio",
                "--window-position=9999,9999",
            ],
            viewport={"width": 1280, "height": 900},
        )

        page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await page.goto(CHATGPT_URL, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(3)
        url = page.url.lower()
        logger.info("chatgpt_web | preflight url=%s", page.url)

        if "auth0" in url or "login" in url or "signin" in url:
            await self._teardown()
            raise RuntimeError(
                "ChatGPT session is not logged in. Please log in at https://chatgpt.com/ and retry."
            )

        if require_5x:
            probe = await self._probe_model(page)
            self._model_probe = probe
            if not probe.get("is_5x", False):
                await self._teardown()
                raise RuntimeError(
                    f"ChatGPT model check failed: expected GPT-5.x, got '{probe.get('raw', '')[:80]}'"
                )

        self._ready = True
        logger.info("chatgpt_web | client ready")

    async def _probe_model(self, page: Page) -> dict[str, Any]:
        result = await _run_query(page, _MODEL_PROMPT, timeout_ms=60_000)
        raw = (result.get("response") or "").strip()
        low = raw.lower()
        hit_4 = bool(re.search(r"gpt[\s\-]?4(\D|$)", low))
        hit_5 = bool(re.search(r"gpt[\s\-]?5(\D|$)", low)) or "gpt-5" in low or "gpt5" in low
        return {"raw": raw, "is_5x": (hit_5 and not hit_4)}

    async def analyze(self, prompt: str, timeout_ms: int = 120_000) -> dict[str, Any]:
        self._assert_ready()
        async with self._semaphore:
            page = await self._context.new_page()
            try:
                return await _run_query(page, prompt, timeout_ms)
            finally:
                await page.close()

    async def analyze_batch(
        self,
        items: list[dict[str, str]],
        timeout_ms: int = 120_000,
    ) -> list[dict[str, Any]]:
        self._assert_ready()

        async def _one(item: dict[str, str]) -> dict[str, Any]:
            try:
                res = await self.analyze(item["prompt"], timeout_ms)
                return {"code": item.get("code", ""), "name": item.get("name", ""), **res}
            except Exception as exc:
                logger.warning("chatgpt_web | batch failed code=%s err=%s", item.get("code"), exc)
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
        logger.info("chatgpt_web | client closed")

    def _assert_ready(self) -> None:
        if not self._ready:
            raise RuntimeError("ChatGPTWebClient is not ready. Call `await ChatGPTWebClient.get()` first.")


async def shutdown() -> None:
    inst = ChatGPTWebClient._instance
    if inst and inst._ready:
        await inst.close()


def _copy_profile(user_data: str, profile: str) -> str:
    src = Path(user_data) / profile
    tmp_root = Path(tempfile.mkdtemp(prefix="chatgpt_svc_"))
    dst = tmp_root / profile

    def _ignore(_, contents):
        return {c for c in contents if c in _SKIP_DIRS}

    if src.exists():
        shutil.copytree(src, dst, ignore=_ignore, dirs_exist_ok=False)
    else:
        dst.mkdir(parents=True, exist_ok=True)

    return str(tmp_root)


async def _run_query(page: Page, prompt: str, timeout_ms: int) -> dict[str, Any]:
    await page.goto(CHATGPT_URL, wait_until="domcontentloaded", timeout=60_000)
    await asyncio.sleep(2)

    ta = page.locator("#prompt-textarea, textarea[data-id='root']").first
    await ta.wait_for(state="visible", timeout=30_000)
    await ta.click()
    await asyncio.sleep(0.2)

    ok = await page.evaluate(
        """(text) => {
            const el = document.querySelector('#prompt-textarea');
            if (!el) return false;
            el.focus();
            const r = document.createRange(); r.selectNodeContents(el);
            const s = window.getSelection(); s.removeAllRanges(); s.addRange(r);
            document.execCommand('insertText', false, text);
            el.dispatchEvent(new InputEvent('input', {bubbles: true}));
            return true;
        }""",
        prompt,
    )
    if not ok:
        await page.keyboard.type(prompt)
    await asyncio.sleep(0.4)

    send = page.locator(_SEND_SEL).first
    try:
        if await send.is_visible(timeout=2500):
            await send.click()
        else:
            await page.keyboard.press("Enter")
    except Exception:
        await page.keyboard.press("Enter")

    t0 = time.time()
    try:
        await page.wait_for_selector(_STOP_SEL, timeout=10_000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(_STOP_SEL, state="hidden", timeout=timeout_ms)
    except Exception:
        pass
    await asyncio.sleep(1.0)
    elapsed = time.time() - t0

    resp: str = await page.evaluate(
        """() => {
            const selectors = [
                '[data-message-author-role="assistant"] .markdown, [data-message-author-role="assistant"]',
                'div[data-testid^="conversation-turn-"] .markdown',
                '.prose'
            ];
            for (const sel of selectors) {
                const els = document.querySelectorAll(sel);
                if (!els.length) continue;
                const text = (els[els.length - 1].innerText || '').trim();
                if (text) return text;
            }
            return (document.body && document.body.innerText) ? document.body.innerText.slice(-4000) : '';
        }"""
    ) or ""

    has_citation = bool(re.search(r"https?://|来源|source|引用", resp, re.I))
    return {"response": resp, "elapsed_s": round(elapsed, 2), "has_citation": has_citation}
