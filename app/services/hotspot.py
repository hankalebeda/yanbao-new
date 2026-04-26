import hashlib
from datetime import datetime, timezone
from typing import Any

import httpx

from app.services.social_browser import fetch_douyin_hot_with_browser
from app.services.source_state import record_source_result


def _topic_id(title: str) -> str:
    return hashlib.md5(title.encode("utf-8")).hexdigest()[:16]


def topic_id_for_title(title: str) -> str:
    return _topic_id(title)


def _safe_datetime(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.now(timezone.utc)


def compute_decay_weight(fetch_time: Any) -> float:
    dt = _safe_datetime(fetch_time)
    hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
    if hours <= 24:
        return 1.0
    if hours <= 72:
        # 24h->72h: 1.0 linearly decays to 0.4
        return round(max(0.4, 1.0 - ((hours - 24.0) / 48.0) * 0.6), 4)
    return 0.2


def score_sentiment(title: str) -> float:
    positive_tokens = ["增长", "利好", "回暖", "突破", "增持", "预增"]
    negative_tokens = ["下滑", "利空", "处罚", "亏损", "减持", "风险"]
    score = 0.0
    for token in positive_tokens:
        if token in title:
            score += 0.25
    for token in negative_tokens:
        if token in title:
            score -= 0.25
    return max(-1.0, min(1.0, score))


def infer_event_type(title: str) -> str:
    mapping = {
        "政策": "policy",
        "财报": "earnings",
        "监管": "regulation",
        "舆情": "public_opinion",
        "产业": "industry_chain",
    }
    for token, event in mapping.items():
        if token in title:
            return event
    return "general"


def _normalize_text(s: str) -> str:
    return (s or "").strip().lower()


def _default_aliases(stock_code: str, stock_name: str | None = None) -> list[str]:
    import json
    from app.core.config import settings

    code = stock_code.split(".")[0]
    aliases = [stock_code.lower(), code]
    if len(code) > 3:
        aliases.append(code[-3:])
    if stock_name:
        aliases.append(stock_name.lower())
    try:
        raw = getattr(settings, "stock_aliases", "") or "{}"
        mapping = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        mapping = {"600519": ["贵州茅台", "茅台", "白酒", "酱香"], "000001": ["平安银行", "银行", "金融"], "300750": ["宁德时代", "动力电池", "锂电"]}
    aliases.extend([x.lower() for x in (mapping.get(code) or [])])
    dedup = []
    seen = set()
    for a in aliases:
        if a and a not in seen:
            dedup.append(a)
            seen.add(a)
    return dedup


async def fetch_weibo_hot(top_n: int = 50):
    url = "https://weibo.com/ajax/side/hotSearch"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://weibo.com/newlogin"}
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        try:
            res = await client.get(url, headers=headers)
            res.raise_for_status()
            rows = res.json().get("data", {}).get("realtime", [])[:top_n]
            result = []
            for i, row in enumerate(rows, start=1):
                title = (row.get("word") or row.get("note") or "").strip()
                if not title:
                    continue
                result.append(
                    {
                        "topic_id": _topic_id(title),
                        "platform": "weibo",
                        "rank": i,
                        "title": title,
                        "raw_heat": str(row.get("num", "")),
                        "fetch_time": datetime.now(timezone.utc),
                        "source_url": "https://s.weibo.com/top/summary",
                        "heat_score": float(max(0, top_n - i + 1)) / float(top_n),
                    }
                )
            record_source_result("hotspot", "weibo", bool(result), None if result else "weibo_empty")
            return result
        except Exception as exc:
            record_source_result("hotspot", "weibo", False, str(exc) or exc.__class__.__name__)
            return []


async def fetch_eastmoney_hot(top_n: int = 50):
    """东方财富财经热榜（无需登录，JSON接口）。失败时返回空列表而非伪造数据。"""
    url = "https://np-aiotopic.eastmoney.com/nolist/hotTopicList"
    params = {"num": min(top_n, 100), "refreshType": "2"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.eastmoney.com/",
    }
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        try:
            res = await client.get(url, params=params, headers=headers)
            res.raise_for_status()
            payload = res.json()
            data = payload.get("data") or []
            if not isinstance(data, list):
                raise RuntimeError("eastmoney_unexpected_format")
            result = []
            for i, row in enumerate(data[:top_n], start=1):
                title = (row.get("topicTitle") or row.get("title") or "").strip()
                if not title:
                    continue
                source_url = (row.get("topicUrl") or row.get("url") or "https://www.eastmoney.com/").strip()
                result.append(
                    {
                        "topic_id": _topic_id(title),
                        "platform": "eastmoney",
                        "rank": i,
                        "title": title,
                        "raw_heat": str(row.get("hotNum") or row.get("heat") or ""),
                        "fetch_time": datetime.now(timezone.utc),
                        "source_url": source_url if source_url.startswith("http") else "https://www.eastmoney.com/",
                        "heat_score": float(max(0, top_n - i + 1)) / float(top_n),
                    }
                )
            record_source_result("hotspot", "eastmoney", bool(result), None if result else "eastmoney_empty")
            return result
        except Exception as exc:
            record_source_result("hotspot", "eastmoney", False, str(exc) or exc.__class__.__name__)
            return []


async def fetch_douyin_hot(top_n: int = 50):
    # Public Douyin endpoint can fail due anti-bot; we return empty instead of fabricated records.
    url = "https://www.douyin.com/aweme/v1/web/hot/search/list/"
    params = {
        "device_platform": "webapp",
        "aid": "6383",
        "channel": "channel_pc_web",
        "pc_client_type": "1",
        "version_code": "190500",
        "detail_list": "1",
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.douyin.com/hot"}
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        try:
            res = await client.get(url, params=params, headers=headers)
            res.raise_for_status()
            word_list = res.json().get("data", {}).get("word_list", [])[:top_n]
            result = []
            for i, row in enumerate(word_list, start=1):
                title = (row.get("word") or row.get("sentence") or "").strip()
                if not title:
                    continue
                result.append(
                    {
                        "topic_id": _topic_id(title),
                        "platform": "douyin",
                        "rank": i,
                        "title": title,
                        "raw_heat": str(row.get("hot_value", "")),
                        "fetch_time": datetime.now(timezone.utc),
                        "source_url": "https://www.douyin.com/hot",
                        "heat_score": float(max(0, top_n - i + 1)) / float(top_n),
                    }
                )
            record_source_result("hotspot", "douyin", bool(result), None if result else "douyin_empty")
            if result:
                return result
        except Exception as exc:
            record_source_result("hotspot", "douyin", False, str(exc) or exc.__class__.__name__)

    # Browser fallback chain
    browser_rows = await fetch_douyin_hot_with_browser(top_n=top_n)
    if browser_rows:
        record_source_result("hotspot", "douyin", True, None)
        return browser_rows
    record_source_result("hotspot", "douyin", False, "douyin_browser_fallback_empty")
    return []


def link_topic_to_stock(
    title: str,
    stock_code: str,
    stock_name: str | None = None,
    industry_keywords: list[str] | None = None,
):
    t = _normalize_text(title)
    aliases = _default_aliases(stock_code, stock_name=stock_name)
    industry = [x.lower() for x in (industry_keywords or []) if x]

    score = 0.0
    methods = []

    code = stock_code.split(".")[0].lower()
    if code in t or stock_code.lower() in t:
        score += 0.8
        methods.append("stock_code")

    for a in aliases:
        if len(a) >= 2 and a in t:
            score += 0.35
            methods.append("alias")
            break

    hit_industry = 0
    for kw in industry:
        if len(kw) >= 2 and kw in t:
            hit_industry += 1
    if hit_industry:
        score += min(0.3, 0.1 * hit_industry)
        methods.append("industry")

    if "公司" in title or "公告" in title:
        score += 0.05

    score = round(max(0.0, min(1.0, score)), 4)
    return {
        "relevance_score": score,
        "match_method": "+".join(methods) if methods else "none",
    }


def enrich_topic(topic: dict):
    title = str(topic.get("title") or topic.get("topic_title") or "").strip()
    sentiment = score_sentiment(title)
    last_price = topic.get("last_price")
    prev_close = topic.get("prev_close")
    shares = topic.get("circulating_shares")

    market_cap = None
    if isinstance(last_price, (int, float)) and isinstance(shares, (int, float)):
        market_cap = float(last_price) * float(shares)

    change_pct = None
    if isinstance(last_price, (int, float)) and isinstance(prev_close, (int, float)) and float(prev_close) != 0:
        change_pct = round((float(last_price) - float(prev_close)) / float(prev_close) * 100.0, 4)

    topic_id = str(topic.get("topic_id") or _topic_id(title or str(topic.get("hotspot_item_id") or "")))

    return {
        "topic_id": topic_id,
        "canonical_topic": title,
        "heat_score": topic["heat_score"],
        "sentiment_score": sentiment,
        "event_type": infer_event_type(title),
        "decay_weight": compute_decay_weight(topic.get("fetch_time")),
        "industry": topic.get("industry"),
        "market_cap": market_cap,
        "change_pct": change_pct,
        "stock_code": topic.get("stock_code"),
        "stock_name": topic.get("stock_name"),
        "last_price": last_price,
    }
