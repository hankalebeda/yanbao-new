"""
GeminiWebClient
===============
Playwright 驱动的 Gemini 网页端客户端（进程单例）。

架构要点
--------
- 单例 BrowserContext：进程内只启动一个 Chrome，多次调用复用会话。
- 每次查询 new_page() 独立标签页 → asyncio.gather 安全并发。
- 信号量限流：最多 MAX_CONCURRENCY 个标签并行，防止 Gemini 限速。
- 首次 analyze() 时惰性初始化，无需手动预热。
- FastAPI lifespan shutdown 时自动调用 shutdown()，释放进程和临时目录。

配置（.env 可覆盖）
-------------------
  GEMINI_CHROME_USER_DATA   Chrome User Data 目录，默认当前用户 Default
  GEMINI_CHROME_PROFILE     Profile 子目录名，默认 Default
  GEMINI_MAX_CONCURRENCY    最大并发标签数，默认 5
"""

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

GEMINI_URL = "https://gemini.google.com/app"

_STOP_SEL = (
    "button[aria-label*='停止'], button[aria-label*='Stop'], "
    "button[data-mat-icon-name='stop_circle']"
)
_SKIP_DIRS = {
    "Cache", "Code Cache", "GPUCache", "DawnCache",
    "GrShaderCache", "ShaderCache", "blob_storage",
    "Service Worker", "CacheStorage", "IndexedDB",
}


# ── 读取配置（延迟 import 避免循环依赖）────────────────────
def _load_config() -> tuple[str, str, int]:
    try:
        from app.core.config import settings
        udata    = getattr(settings, "gemini_chrome_user_data", "") or _default_udata()
        profile  = getattr(settings, "gemini_chrome_profile", "")  or "Default"
        max_conc = int(getattr(settings, "gemini_max_concurrency", 0) or 5)
    except Exception:
        udata, profile, max_conc = _default_udata(), "Default", 5
    return udata, profile, max_conc


def _default_udata() -> str:
    return os.path.expandvars(r"C:\Users\%USERNAME%\AppData\Local\Google\Chrome\User Data")


# ── 进程单例 ────────────────────────────────────────────────
class GeminiWebClient:
    """Playwright 驱动的 Gemini 网页端分析客户端（进程单例）。"""

    _instance: "GeminiWebClient | None" = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._pw          = None
        self._context: BrowserContext | None = None
        self._tmp_dir: str | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._ready       = False

    # ── 获取单例 ──────────────────────────────────────────
    @classmethod
    async def get(cls) -> "GeminiWebClient":
        """获取进程单例；首次调用自动初始化浏览器（约 10s）。"""
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            inst = cls._instance
            if not inst._ready:
                await inst._init()
            return inst

    # ── 初始化 ────────────────────────────────────────────
    async def _init(self) -> None:
        user_data, profile, max_conc = _load_config()
        self._semaphore = asyncio.Semaphore(max_conc)

        logger.info("gemini_web | copying Chrome profile %s/%s", user_data, profile)
        self._tmp_dir = _copy_profile(user_data, profile)

        logger.info("gemini_web | launching Chrome (offscreen)")
        self._pw = await async_playwright().start()
        self._context = await self._pw.chromium.launch_persistent_context(
            user_data_dir=self._tmp_dir,
            channel="chrome",
            headless=False,                        # Google 会拦截 headless
            args=[
                f"--profile-directory={profile}",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-notifications",
                "--mute-audio",
                "--window-position=9999,9999",     # 推到屏幕外
            ],
            viewport={"width": 1280, "height": 900},
        )

        # 预热：验证登录
        page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(3)
        url = page.url
        logger.info("gemini_web | preflight url=%s", url)

        if "accounts.google.com" in url or "signin" in url.lower():
            await self._teardown()
            raise RuntimeError(
                "Gemini 会话未登录：Cookie 已过期或被 Google 拒绝。"
                "请在本机 Chrome 重新登录 gemini.google.com，再重启服务。"
            )

        self._ready = True
        logger.info("gemini_web | client ready")

    # ── 单条分析 ──────────────────────────────────────────
    async def analyze(self, prompt: str, timeout_ms: int = 120_000) -> dict[str, Any]:
        """
        向 Gemini 发送一条 prompt，等待完整回复后返回。

        Returns
        -------
        dict with keys:
            response    (str)   完整回复文本
            elapsed_s   (float) 生成耗时（秒）
            has_citation(bool)  回复中是否含来源链接/引用
        """
        self._assert_ready()
        async with self._semaphore:
            page = await self._context.new_page()
            try:
                return await _run_query(page, prompt, timeout_ms)
            finally:
                await page.close()

    # ── 并发批量分析 ──────────────────────────────────────
    async def analyze_batch(
        self,
        items: list[dict[str, str]],
        timeout_ms: int = 120_000,
    ) -> list[dict[str, Any]]:
        """
        并发分析多只股票，结果顺序与 items 一致。

        Parameters
        ----------
        items : list of {"code": str, "name": str, "prompt": str}

        Returns
        -------
        list of {"code", "name", "response", "elapsed_s", "has_citation"}
        失败项含 "error" 字段，不影响其他结果。
        """
        self._assert_ready()

        async def _one(item: dict) -> dict:
            try:
                res = await self.analyze(item["prompt"], timeout_ms)
                return {"code": item.get("code", ""), "name": item.get("name", ""), **res}
            except Exception as exc:
                logger.warning("gemini_web | batch failed code=%s err=%s", item.get("code"), exc)
                return {"code": item.get("code", ""), "name": item.get("name", ""), "error": str(exc)}

        return list(await asyncio.gather(*[_one(it) for it in items]))

    # ── 关闭 ──────────────────────────────────────────────
    async def close(self) -> None:
        """释放 Chrome 进程和临时目录；下次 analyze 时自动重新初始化。"""
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
        logger.info("gemini_web | client closed")

    def _assert_ready(self) -> None:
        if not self._ready:
            raise RuntimeError(
                "GeminiWebClient 未就绪，请先 await GeminiWebClient.get()。"
            )


# ── FastAPI lifespan 钩子 ───────────────────────────────────
async def shutdown() -> None:
    """在 FastAPI lifespan shutdown 阶段调用，优雅关闭浏览器。"""
    inst = GeminiWebClient._instance
    if inst and inst._ready:
        await inst.close()


# ── 内部工具 ────────────────────────────────────────────────
def _copy_profile(user_data: str, profile: str) -> str:
    src      = Path(user_data) / profile
    tmp_root = Path(tempfile.mkdtemp(prefix="gemini_svc_"))
    dst      = tmp_root / profile

    def _ignore(_, contents):
        return {c for c in contents if c in _SKIP_DIRS}

    if src.exists():
        shutil.copytree(src, dst, ignore=_ignore, dirs_exist_ok=False)
        logger.debug("gemini_web | profile → %s", tmp_root)
    else:
        logger.warning("gemini_web | profile not found at %s, using empty dir", src)
        dst.mkdir(parents=True, exist_ok=True)

    return str(tmp_root)


async def _enable_web_search(page: Page) -> bool:
    """尝试点击 Gemini 工具栏的联网按钮。"""
    for sel in [
        "button[aria-label*='搜索']", "button[aria-label*='Search']",
        "button[aria-label*='Google Search']", "button[aria-label*='联网']",
        "[data-mat-icon-name='travel_explore']", "[data-mat-icon-name='search']",
        "button:has-text('使用 Google 搜索')", "button:has-text('Use Google Search')",
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1500):
                if await btn.get_attribute("aria-pressed") != "true":
                    await btn.click()
                    await asyncio.sleep(0.4)
                return True
        except Exception:
            pass
    return False


async def _run_query(page: Page, prompt: str, timeout_ms: int) -> dict[str, Any]:
    """在独立标签页上打开 Gemini，注入 prompt，等待回复完成。"""
    await page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=30_000)
    await asyncio.sleep(1.5)
    await _enable_web_search(page)

    # 定位输入框
    ta = page.locator(
        "div[contenteditable='true'], rich-textarea [contenteditable='true']"
    ).first
    await ta.wait_for(state="visible", timeout=20_000)
    await ta.click()
    await asyncio.sleep(0.3)

    # JS 注入（绕过 Windows GBK 中文乱码）
    ok = await page.evaluate(
        """(text) => {
            const el = document.querySelector(
                'div[contenteditable="true"], rich-textarea [contenteditable="true"]'
            );
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

    # 发送
    send = page.locator(
        "button[aria-label*='发送'], button[aria-label*='Send'], "
        "button[jsname='Qj595b'], button[data-mat-icon-name='send']"
    ).first
    try:
        if await send.is_visible(timeout=3000):
            await send.click()
        else:
            await page.keyboard.press("Enter")
    except Exception:
        await page.keyboard.press("Enter")

    # 等待生成完毕（stop 出现 → 消失）
    t0 = time.time()
    try:
        await page.wait_for_selector(_STOP_SEL, timeout=12_000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(_STOP_SEL, state="hidden", timeout=timeout_ms)
    except Exception:
        pass
    elapsed = time.time() - t0
    await asyncio.sleep(1.0)

    # 抓取回复
    resp: str = await page.evaluate(
        """() => {
            for (const s of [
                'model-response .markdown', '.model-response-text',
                'message-content .content', '[class*="model-response"] .markdown',
                'response-container .markdown',
            ]) {
                const els = document.querySelectorAll(s);
                if (els.length) return els[els.length - 1].innerText;
            }
            return document.body.innerText.slice(-4000);
        }"""
    ) or ""

    has_citation = bool(re.search(r"https?://|来源|source|引用|据.*报", resp, re.I))
    logger.debug("gemini_web | done elapsed=%.1fs len=%d citation=%s", elapsed, len(resp), has_citation)
    return {"response": resp, "elapsed_s": round(elapsed, 2), "has_citation": has_citation}
