import re
from datetime import datetime, timezone

from app.core.config import settings


def _topic_id(text: str) -> str:
    import hashlib

    return hashlib.md5(text.encode("utf-8")).hexdigest()[:16]


def _clean(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


async def fetch_douyin_hot_with_browser(top_n: int = 50) -> list[dict]:
    if not settings.enable_browser_fallback:
        return []

    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception:
        return []

    out: list[dict] = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto("https://www.douyin.com/hot", timeout=settings.browser_fallback_timeout_seconds * 1000)
                await page.wait_for_timeout(3500)
                texts = await page.eval_on_selector_all(
                    "a, span",
                    """
                    (els) => els
                      .map(e => (e.innerText || '').trim())
                      .filter(t => t.length >= 4 && t.length <= 40)
                    """,
                )

                # Keep lines likely to be hot topics; remove duplicates/noise.
                seen = set()
                for t in texts:
                    c = _clean(t)
                    if not c or c in seen:
                        continue
                    if re.search(r"登录|下载|打开|推荐|关注|点赞|评论|分享", c):
                        continue
                    seen.add(c)
                    out.append(
                        {
                            "topic_id": _topic_id(c),
                            "platform": "douyin",
                            "rank": len(out) + 1,
                            "title": c,
                            "raw_heat": "",
                            "fetch_time": datetime.now(timezone.utc),
                            "source_url": "https://www.douyin.com/hot",
                            "heat_score": float(max(0, top_n - len(out))) / float(top_n),
                        }
                    )
                    if len(out) >= top_n:
                        break
            finally:
                await browser.close()
    except Exception:
        return []

    return out
