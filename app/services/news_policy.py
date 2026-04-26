import html
import json
import re
from asyncio import gather
from datetime import datetime, timezone

import httpx


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(text or "")).strip()


def _pack(source_name: str, source_url: str, title: str, category: str) -> dict:
    return {
        "source_name": source_name,
        "source_url": source_url,
        "title": title,
        "fetch_time": datetime.now(timezone.utc).isoformat(),
        "category": category,
    }


def _is_low_quality_news_title(title: str) -> bool:
    t = (title or "").strip()
    bad_keywords = [
        "开户",
        "交易",
        "Level-2",
        "金融终端",
        "广告",
        "下载",
        "APP",
        "优惠",
        "首页",
        "要闻",
        "公司·产经",
        "市场·港股",
        "基金·ETF",
    ]
    return any(k in t for k in bad_keywords)


def _is_stock_related(title: str, stock_code: str, stock_name: str | None, keywords: list[str] | None) -> bool:
    t = (title or "").strip()
    if not t:
        return False
    code = stock_code.split(".")[0]
    if code in t:
        return True
    if stock_name and stock_name in t:
        return True
    for k in keywords or []:
        if k and k in t:
            return True
    return False


async def _fetch_stock_news_eastmoney(
    stock_code: str,
    stock_name: str | None = None,
    keywords: list[str] | None = None,
    limit: int = 5,
) -> list[dict]:
    code = stock_code.split(".")[0]
    url = f"https://so.eastmoney.com/web/s?keyword={code}"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            text = resp.text
            matches = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', text, flags=re.I | re.S)
            out = []
            for href, title_html in matches:
                href = href.strip()
                if not href.startswith("http"):
                    continue
                title = _clean(re.sub(r"<.*?>", "", title_html))
                if len(title) < 8 or _is_low_quality_news_title(title):
                    continue
                if not _is_stock_related(title, stock_code, stock_name, keywords):
                    continue
                out.append(_pack("东方财富搜索", href, title, "news"))
                if len(out) >= limit:
                    break
            return out
        except Exception:
            return []


async def _fetch_stock_news_cs(
    stock_code: str,
    stock_name: str | None = None,
    keywords: list[str] | None = None,
    limit: int = 5,
) -> list[dict]:
    # 中证网 https://www.cs.com.cn/ssgs/ does NOT include the stock code in the URL
    # and returns a generic page for ALL stocks — it cannot be filtered per-stock.
    # Replaced with a second pass of Eastmoney news search to avoid fake/irrelevant results.
    return await _fetch_stock_news_eastmoney(
        stock_code, stock_name=stock_name, keywords=keywords, limit=limit
    )


async def _fetch_stock_announcements_eastmoney(stock_code: str, limit: int = 5) -> list[dict]:
    code = stock_code.split(".")[0]
    url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
    params = {
        "cb": "jQuery",
        "sr": "-1",
        "page_size": str(max(limit, 5)),
        "page_index": "1",
        "ann_type": "A",
        "client_source": "web",
        "stock_list": code,
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/notices/"}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            m = re.search(r"\((\{.*\})\)\s*$", resp.text.strip(), flags=re.S)
            if not m:
                return []
            payload = json.loads(m.group(1))
            rows = ((payload.get("data") or {}).get("list") or [])[:limit]
            out = []
            for row in rows:
                title = _clean(row.get("title") or "")
                art_code = str(row.get("art_code") or "").strip()
                if not title or not art_code:
                    continue
                source_url = f"https://data.eastmoney.com/notices/detail/{code}/{art_code}.html"
                out.append(_pack("东方财富公告", source_url, title, "news"))
            return out
        except Exception:
            return []


async def fetch_stock_news(stock_code: str, stock_name: str | None = None, keywords: list[str] | None = None, limit: int = 5) -> list[dict]:
    a, b, c = await gather(
        _fetch_stock_announcements_eastmoney(stock_code, limit=limit),
        _fetch_stock_news_eastmoney(stock_code, stock_name=stock_name, keywords=keywords, limit=limit),
        _fetch_stock_news_cs(stock_code, stock_name=stock_name, keywords=keywords, limit=limit),
    )
    merged = a + b + c
    seen = set()
    out = []
    for i in merged:
        if i["source_url"] in seen:
            continue
        seen.add(i["source_url"])
        out.append(i)
        if len(out) >= limit:
            break
    return out


async def _fetch_policy_from_csrc(limit: int = 5) -> list[dict]:
    url = "https://www.csrc.gov.cn/csrc/c100028/common_list.shtml"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            text = resp.text
            matches = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', text, flags=re.I | re.S)
            out = []
            for href, title_html in matches:
                href = href.strip()
                if href.startswith("/"):
                    href = f"https://www.csrc.gov.cn{href}"
                t = _clean(re.sub(r"<.*?>", "", title_html))
                if not href.startswith("http") or not t:
                    continue
                if "c100028" not in href and "content.shtml" not in href:
                    continue
                out.append(_pack("证监会", href, t, "policy"))
                if len(out) >= limit:
                    break
            return out
        except Exception:
            return []


async def _fetch_policy_from_gov(limit: int = 5) -> list[dict]:
    url = "https://www.gov.cn/zhengce/zuixin.htm"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            text = resp.text
            matches = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>([^<]{8,120})</a>', text, flags=re.I)
            out = []
            for href, title in matches:
                href = href.strip()
                if href.startswith("/"):
                    href = f"https://www.gov.cn{href}"
                t = _clean(title)
                if not href.startswith("http") or not t:
                    continue
                out.append(_pack("中国政府网", href, t, "policy"))
                if len(out) >= limit:
                    break
            return out
        except Exception:
            return []


async def fetch_policy_news(limit: int = 8) -> list[dict]:
    csrc, gov = await gather(
        _fetch_policy_from_csrc(limit=limit),
        _fetch_policy_from_gov(limit=limit),
    )
    merged = csrc + gov
    seen = set()
    out = []
    for item in merged:
        key = item["source_url"]
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= limit:
            break
    return out