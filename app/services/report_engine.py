import logging
import re
from asyncio import gather
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from app.core.config import settings
from app.models import (
    EnhancementExperiment,
    HotspotNormalized,
    HotspotRaw,
    HotspotStockLink,
    MarketHotspotItem,
    MarketHotspotItemStockLink,
    ModelRunLog,
    PredictionOutcome,
    Report,
    ReportFeedback,
    ReportIdempotency,
)
from app.services.company_data import fetch_company_overview, fetch_industry_competition, fetch_valuation_snapshot
from app.services.capital_flow import fetch_capital_dimensions
from app.services.hotspot import (
    compute_decay_weight,
    enrich_topic,
    fetch_douyin_hot,
    fetch_weibo_hot,
    link_topic_to_stock,
    topic_id_for_title,
)
from app.services.market_state import get_cached_market_state
from app.services.market_data import (
    fetch_market_features,
    fetch_price_return,
    fetch_price_return_from_trade_date,
    fetch_quote_snapshot,
)
from app.services.news_policy import _is_low_quality_news_title, fetch_policy_news, fetch_stock_news
from app.services.ollama_client import ollama_client
from app.services.tdx_local_data import build_features_series, build_tdx_local_features, load_market_breadth_latest
from app.services.trade_calendar import latest_trade_date_str

def trade_date_str(dt: datetime | None = None) -> str:
    return latest_trade_date_str(dt)


def _fmt_yuan(val, default: str = "数据缺失") -> str:
    """Format a yuan-denominated number to human-readable Chinese financial units.

    East Money API raw values are in yuan (元).
    Converts to 亿 (>= 1e8), 万 (>= 1e4), or 元 (otherwise).
    Returns a signed string like '+2.3亿' or '-1580万'.
    """
    if val is None or not isinstance(val, (int, float)):
        return default
    v = float(val)
    sign = "+" if v >= 0 else ""
    abs_v = abs(v)
    if abs_v >= 1e8:
        return f"{sign}{v/1e8:.2f}亿元"
    if abs_v >= 1e4:
        return f"{sign}{v/1e4:.1f}万元"
    if abs_v >= 1:
        return f"{sign}{v:.0f}元"
    # Extremely small value — likely a ratio/percentage, not yuan
    return f"{sign}{v:.4f}（非元单位）"


def daily_idempotency_key(stock_code: str, dt: datetime | None = None) -> str:
    return f"daily:{stock_code}:{trade_date_str(dt)}"


def _sanitize_llm_summary(text: str) -> str:
    t = re.sub(r"[*`#>\-]", "", text or "")
    t = re.sub(r"\s+", " ", t).strip()
    # Strip raw JSON prefix that leaks into summary (e.g. "json {..." or "{\"recommendation\"...")
    t = re.sub(r'^json\s*\{.*', '', t, flags=re.DOTALL).strip()
    t = re.sub(r'^\{.*?"recommendation"\s*:\s*"[^"]*".*', '', t, flags=re.DOTALL).strip()
    if len(t) > 180:
        t = t[:180] + "..."
    return t


def _generate_unicode_sparkline(series: list[float | None]) -> str:
    valid = [x for x in series if isinstance(x, (int, float))]
    if not valid:
        return ""
    mn, mx = min(valid), max(valid)
    if mx == mn:
        return "▃" * len(valid)
    
    chars = "  ▂▃▅▆▇"
    out = []
    for x in valid:
        idx = int((x - mn) / (mx - mn) * (len(chars) - 1))
        out.append(chars[idx])
    return "".join(out)


def _preprocess_prompt_context(
    stock_code: str,
    market_snapshot: dict,
    market_features: dict,
    tdx_local: dict,
    capital_dims: dict,
    social_items: list[dict],
    news_items: list[dict],
    policy_items: list[dict],
) -> str:
    parts = []
    
    # 1. Market Snapshot
    last = market_snapshot.get("last_price")
    pct = market_snapshot.get("pct_change")
    parts.append(f"【行情】当前价: {last}, 涨跌幅: {pct if pct is not None else 'N/A'}")
    
    # 2. Technical Features (Natural Language)
    feat = (tdx_local or {}).get("features", {})
    trend = market_features.get("features", {}).get("trend", "震荡")
    ma5 = market_features.get("features", {}).get("ma5")
    ma20 = market_features.get("features", {}).get("ma20")
    
    tech_desc = [f"短期趋势: {trend}", f"均线状态: MA5({ma5}) vs MA20({ma20})"]
    
    # MACD
    macd = feat.get("macd") or {}
    dif = macd.get("dif")
    dea = macd.get("dea")
    if isinstance(dif, (int, float)) and isinstance(dea, (int, float)):
        cross = "金叉" if dif > dea else "死叉"
        pos = "零轴上方" if dif > 0 else "零轴下方"
        tech_desc.append(f"MACD: {cross}, 位于{pos}")
    
    # KDJ
    kdj = feat.get("kdj") or {}
    k, d, j = kdj.get("k"), kdj.get("d"), kdj.get("j")
    if isinstance(k, (int, float)) and isinstance(d, (int, float)) and isinstance(j, (int, float)):
        kdj_st = "超买" if max(k,d,j) > 80 else ("超卖" if min(k,d,j) < 20 else "常态")
        tech_desc.append(f"KDJ: {kdj_st} (J={round(j,1)})")

    # BOLL
    boll = feat.get("boll") or {}
    upper, mid, lower = boll.get("upper"), boll.get("mid"), boll.get("lower")
    if isinstance(last, (int, float)) and isinstance(upper, (int, float)) and isinstance(lower, (int, float)):
        if last > upper:
            boll_st = "突破上轨"
        elif last < lower:
            boll_st = "跌破下轨"
        else:
            boll_st = "中轨运行"
        tech_desc.append(f"BOLL: {boll_st}")
        
    parts.append("【技术面】" + "; ".join(tech_desc))
    
    # 3. Capital Flow (Graceful Degradation)
    cap = capital_dims.get("capital_flow", {}) if capital_dims else {}
    mf = cap.get("main_force", {})
    lhb = (capital_dims or {}).get("dragon_tiger", {})
    
    cap_desc = []
    net5 = mf.get("net_inflow_5d")
    if net5 is not None:
        cap_desc.append(f"主力5日净流: {_fmt_yuan(net5)}")
    else:
        cap_desc.append("主力资金净流: 暂无（TDX代理估算未返回数据）")
        
    if lhb.get("status") == "ok":
        cnt = lhb.get("lhb_count_30d", 0)
        net = lhb.get("net_buy_total")
        cap_desc.append(f"龙虎榜: 近30日上榜{cnt}次, 总净买{net}")
    else:
        cap_desc.append("龙虎榜: 近期未上榜")
        
    parts.append("【资金面】" + "; ".join(cap_desc))
    
    # 4. News & Social
    parts.append(f"【消息面】新闻{len(news_items)}条, 政策{len(policy_items)}条, 热搜{len(social_items)}条")
    
    return "\n".join(parts)


def build_prompt(
    stock_code: str,
    market_snapshot: dict,
    market_features: dict,
    social_items: list[dict],
    news_items: list[dict],
    policy_items: list[dict],
    tdx_local: dict = None,
    capital_dims: dict = None,
) -> str:
    context = _preprocess_prompt_context(
        stock_code=stock_code,
        market_snapshot=market_snapshot,
        market_features=market_features,
        tdx_local=tdx_local,
        capital_dims=capital_dims,
        social_items=social_items,
        news_items=news_items,
        policy_items=policy_items,
    )
    news_titles = [x.get("title") for x in news_items[:5] if x.get("title")]
    policy_titles = [x.get("title") for x in policy_items[:3] if x.get("title")]
    social_titles = [x.get("title") for x in social_items[:3] if x.get("title")]
    return (
        "你是专业A股研报分析师，请综合以下多维度数据，给出当前操作建议。"
        "分析要结合技术面、资金面和消息面，理由要具体详实，不要泛泛而谈。"
        "请严格按以下JSON格式输出，所有字段必须填写，不要输出任何其他文字：\n"
        '{"recommendation":"买入或卖出或观望",'
        '"reason":"详细核心理由（3-5句，结合当前技术指标状态、资金流向、近期消息）",'
        '"trigger":"建议操作的具体触发条件（价格突破位或信号变化）",'
        '"invalidation":"建议失效条件（何种情况应止损或撤单）",'
        '"risks":"主要风险（2-3条）"}\n\n'
        f"股票代码：{stock_code}\n"
        f"{context}\n\n"
        f"【近期新闻】{'；'.join(news_titles) or '无'}\n"
        f"【政策动向】{'；'.join(policy_titles) or '无'}\n"
        f"【市场热议】{'；'.join(social_titles) or '无'}"
    )


def _recommendation_from_text(text: str) -> str:
    import json as _json
    # Try to parse JSON response first
    try:
        # Find JSON in text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            obj = _json.loads(text[start:end])
            reco = obj.get("recommendation", "")
            if "卖" in reco:
                return "SELL"
            if "买" in reco:
                return "BUY"
            if reco:
                return "HOLD"
    except (ValueError, KeyError):
        pass
    # Fallback to keyword matching
    if "卖" in text:
        return "SELL"
    if "买" in text:
        return "BUY"
    return "HOLD"


def _rule_recommendation(market_features: dict) -> str:
    f = market_features.get("features", {})
    trend = f.get("trend")
    ret5 = f.get("ret5")
    if trend == "偏多" and ret5 is not None and ret5 > 0:
        return "BUY"
    if trend == "偏空" and ret5 is not None and ret5 < 0:
        return "SELL"
    return "HOLD"


def _judge_correct(recommendation: str, actual_return: float | None, volatility_20d: float | None = None) -> tuple[int | None, str | None]:
    if actual_return is None:
        return None, "missing_market_data"
    # Dynamic tolerance bands based on individual stock volatility
    vol = volatility_20d or 0.02
    noise_band = max(0.003, min(0.01, vol * 0.3))   # BUY/SELL noise tolerance
    hold_band = max(0.015, min(0.035, vol * 1.2))    # HOLD oscillation tolerance
    if recommendation == "BUY":
        ok = actual_return > -noise_band   # Not crashing = correct
    elif recommendation == "SELL":
        ok = actual_return < noise_band    # Not surging = correct
    else:
        ok = abs(actual_return) <= hold_band  # A-share 2-3% is normal oscillation
    return (1 if ok else 0), (None if ok else "direction")


def _cn_analysis(market_snapshot: dict, market_features: dict) -> str:
    last = market_snapshot.get("last_price")
    pct = market_snapshot.get("pct_change")
    f = market_features.get("features", {})
    trend = f.get("trend", "未知")
    ma5 = f.get("ma5")
    ma20 = f.get("ma20")
    ret5 = f.get("ret5")
    return (
        f"当前价格 {last}，当日涨跌幅 {pct}；"
        f"MA5(近5日均价)={ma5}, MA20(近20日均价)={ma20}，趋势 {trend}；近5日收益 {ret5}。"
    )


def _factor_score(x: float, scale: float = 1.0, clip: float = 1.0) -> float:
    if scale == 0:
        return 0.0
    v = x / scale
    if v > clip:
        return clip
    if v < -clip:
        return -clip
    return v


def _parse_date_yyyymmdd(s: str | None) -> datetime | None:
    t = str(s or "").strip()
    if len(t) != 10:
        return None
    try:
        return datetime.fromisoformat(t + "T00:00:00+00:00")
    except Exception:
        return None


def _days_since_listed(listed_date: str | None) -> int | None:
    d = _parse_date_yyyymmdd(listed_date)
    if not d:
        return None
    return max(0, int((datetime.now(timezone.utc) - d).days))


def _has_meaningful_value(v) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip() != ""
    if isinstance(v, (list, tuple, set, dict)):
        return len(v) > 0
    return True


def _has_any_required_value(obj: dict, required_keys: list[str] | None = None) -> bool:
    if not isinstance(obj, dict) or not obj:
        return False
    if required_keys:
        return any(_has_meaningful_value(obj.get(k)) for k in required_keys)
    return any(_has_meaningful_value(v) for v in obj.values())


def _should_rebuild_daily_idempotent_report(payload: dict, created_at: datetime | None) -> bool:
    if not isinstance(payload, dict):
        return False
    cooldown = max(0, int(settings.daily_idem_rebuild_after_minutes))
    if cooldown > 0 and created_at:
        created = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - created.astimezone(timezone.utc)).total_seconds() / 60.0
        if age_min < cooldown:
            return False

    co = payload.get("company_overview") or {}
    va = payload.get("valuation") or {}
    cg = payload.get("capital_game") or {}
    mf_status = ((cg.get("main_force") or {}).get("status"))
    margin_status = ((cg.get("margin_financing") or {}).get("status"))

    core_missing = (
        not _has_meaningful_value(co.get("industry"))
        or not _has_meaningful_value(co.get("listed_date"))
        or va.get("pe_ttm") is None
        or va.get("pb") is None
        or mf_status not in {"ok", "stale_ok"}
        or margin_status not in {"ok", "stale_ok"}
    )
    return core_missing


def _signal_value(
    model_name: str,
    closes: list[float],
    i: int,
    features_series: list[dict] | None = None,
    horizon_days: int | None = None,
) -> float:
    """信号值：正=看多，负=看空，0=无信号。horizon_days 为预测周期时，动量类使用「过去 h 日收益」与预测「未来 h 日收益」对齐，否则用固定 5/10/20 日。"""
    if i < 21:
        return 0.0
    c = closes[i]
    # 与预测周期对齐：预测未来 h 日收益时，用过去 h 日收益作为信号（动量持续性）
    h_lookback = min(horizon_days, i) if horizon_days and horizon_days >= 1 else None
    if h_lookback and h_lookback >= 1:
        c_h = closes[i - h_lookback] if (i - h_lookback) >= 0 and closes[i - h_lookback] != 0 else 0.0
        ret_h = (c - c_h) / c_h if c_h else 0.0
    else:
        ret_h = None
    c5 = closes[i - 5] if i >= 5 and closes[i - 5] != 0 else 0.0
    c10 = closes[i - 10] if i >= 10 and closes[i - 10] != 0 else 0.0
    c20 = closes[i - 20] if i >= 20 and closes[i - 20] != 0 else 0.0
    ret5 = (c - c5) / c5 if c5 else 0.0
    ret10 = (c - c10) / c10 if c10 else 0.0
    ret20 = (c - c20) / c20 if c20 else 0.0
    ma20 = sum(closes[i - 20 : i]) / 20.0 if i >= 20 else c
    mean_revert = -((c - ma20) / ma20) if ma20 > 0 else 0.0
    hi20 = max(closes[i - 20 : i]) if i >= 20 else c
    lo20 = min(closes[i - 20 : i]) if i >= 20 else c
    breakout = ((c - hi20) / hi20) if hi20 > 0 else 0.0
    bounce = ((c - lo20) / lo20) if lo20 > 0 else 0.0
    # 动量类：有 horizon 时全部用 ret_h（过去 h 日收益预测未来 h 日）
    # 三个动量模型信号相同，但在回测中会与 breakout/bounce/mean_revert 混合后产生差异
    r5 = ret_h if ret_h is not None else ret5
    r10 = ret_h if ret_h is not None else ret10
    r20 = ret_h if ret_h is not None else ret20

    # --- Momentum models (diversified blend when horizon_days set) ---
    if model_name == "五日动量":
        return r5
    if model_name == "十日动量":
        return r10
    if model_name == "二十日动量":
        return r20
    if model_name == "均值回归":
        return mean_revert
    if model_name == "动量回归混合":
        return 0.6 * mean_revert + 0.4 * r5
    if model_name == "突破延续":
        return 0.6 * breakout + 0.4 * r10
    if model_name == "低位反弹":
        return 0.6 * bounce + 0.4 * mean_revert

    # --- Technical indicator models (require features_series) ---
    fs = features_series[i] if features_series and i < len(features_series) else None
    if fs is None:
        return 0.0

    if model_name == "RSI反转":
        # RSI14 with strict extreme threshold.
        # RSI14 < 25: deeply oversold — much rarer and more reliable than RSI14 < 30.
        # Also require RSI14 was above 40 in last 20 days (oscillating, not sustained trend).
        rsi14 = fs.get("rsi14", 50.0)
        if rsi14 < 25 and features_series and i >= 20:
            recent_rsi14 = [features_series[j].get("rsi14", 50.0) for j in range(i - 20, i)]
            was_oscillating = any(r > 40 for r in recent_rsi14)
            if was_oscillating:
                return 1.0
        if rsi14 > 75 and features_series and i >= 20:
            recent_rsi14 = [features_series[j].get("rsi14", 50.0) for j in range(i - 20, i)]
            was_oscillating = any(r < 60 for r in recent_rsi14)
            if was_oscillating:
                return -1.0
        return 0.0

    if model_name == "MACD金死叉":
        dif = fs.get("macd_dif", 0.0)
        dea = fs.get("macd_dea", 0.0)
        prev_fs = features_series[i - 1] if features_series and i > 0 else None
        if prev_fs is None:
            return 0.0
        prev_dif = prev_fs.get("macd_dif", 0.0)
        prev_dea = prev_fs.get("macd_dea", 0.0)
        if prev_dif <= prev_dea and dif > dea:
            return 1.0
        if prev_dif >= prev_dea and dif < dea:
            return -1.0
        return 0.0

    if model_name == "KDJ超买超卖":
        j = fs.get("kdj_j", 50.0)
        if j < 10:
            return 1.0
        if j > 90:
            return -1.0
        return 0.0

    if model_name == "BOLL通道":
        upper = fs.get("boll_upper")
        lower = fs.get("boll_lower")
        if upper is None or lower is None:
            return 0.0
        if c < lower:
            return 1.0
        if c > upper:
            return -1.0
        return 0.0

    if model_name == "多指标共振":
        # High-precision reversal signal combining BOLL extreme breach, RSI14, volume, and
        # trend-regime filter. Fires rarely (~1-3% of days) but with genuinely higher accuracy.
        # Key insight: mean-reversion only works in ranging/oscillating markets, NOT in
        # sustained trends. The trend filter prevents trading against a structural move.
        upper = fs.get("boll_upper")
        lower = fs.get("boll_lower")
        mid = fs.get("boll_mid")
        rsi14 = fs.get("rsi14", 50.0)
        vol_ratio = fs.get("vol_ratio", 1.0)

        if upper is None or lower is None or mid is None:
            return 0.0

        half_band = mid - lower  # = 2σ
        lower3 = lower - 0.5 * half_band   # = mid - 3σ (extreme oversold)
        upper3 = upper + 0.5 * half_band   # = mid + 3σ (extreme overbought)

        boll3_buy = c < lower3
        boll3_sell = c > upper3
        boll2_buy = c < lower
        boll2_sell = c > upper

        # Trend-regime filter: only allow reversal signals in oscillating markets.
        # If the stock has been in a sustained downtrend (60d return < -20%), a BOLL
        # lower breach is more likely a "falling knife" than a reversal opportunity.
        trend_allows_buy = True
        trend_allows_sell = True
        if features_series and i >= 60 and closes[i - 60] > 0:
            ret60 = (c - closes[i - 60]) / closes[i - 60]
            trend_allows_buy = ret60 > -0.20   # not a structural collapse
            trend_allows_sell = ret60 < 0.20   # not a parabolic run

        # 3σ breach + trend filter
        if boll3_buy and trend_allows_buy:
            return 1.0
        if boll3_sell and trend_allows_sell:
            return -1.0

        # 2σ breach + RSI14 extreme + volume spike + trend filter
        vol_spike = vol_ratio >= 2.0
        if boll2_buy and rsi14 < 30 and vol_spike and trend_allows_buy:
            return 0.9
        if boll2_sell and rsi14 > 70 and vol_spike and trend_allows_sell:
            return -0.9

        # 2σ breach + RSI14 very extreme (no volume required) + trend filter
        if boll2_buy and rsi14 < 22 and trend_allows_buy:
            return 0.8
        if boll2_sell and rsi14 > 78 and trend_allows_sell:
            return -0.8

        return 0.0

    return 0.0


def _backtest_forecast_model(tdx_local: dict, stock_code: str = "", features_series: list | None = None) -> dict:
    """占位：技术信号历史命中率已废弃，统一使用动量回归混合，不再执行滚动回测。"""
    windows = [1, 7, 14, 30, 60]
    selected = [
        {
            "horizon_days": h,
            "model_name": "动量回归混合",
            "accuracy_used": None,
            "samples_used": 0,
            "sample_scope": None,
        }
        for h in windows
    ]
    return {
        "horizons": [],
        "horizons_recent_3m": [],
        "summary": {},
        "summary_recent_3m": {},
        "model_candidates": [{"model_name": "动量回归混合"}],
        "selected_model_by_horizon": selected,
    }


def _parse_llm_forecast(text: str) -> dict:
    """Parse LLM forecast JSON output into a structured dict keyed by horizon label.

    Handles:
    - Markdown code fences (```json ... ```)
    - deepseek-r1 <think>...</think> reasoning blocks
    - Extra whitespace / newlines
    - Unescaped control characters (newlines, tabs) inside JSON string values
    - Truncated JSON (extracts completed horizon keys even if overall JSON is cut off)
    """
    import json as _json
    import re as _re
    if not text:
        return {}
    # Strip chain-of-thought <think>...</think> blocks (deepseek-r1)
    clean = _re.sub(r"<think>.*?</think>", "", text, flags=_re.DOTALL).strip()
    # Strip markdown code fences (``` or ```json)
    clean = _re.sub(r"```(?:json)?[ \t]*", "", clean).strip()

    def _try_parse(s: str) -> dict | None:
        """Try standard parse, then fix unescaped control chars."""
        try:
            return _json.loads(s)
        except _json.JSONDecodeError:
            try:
                fixed = _re.sub(
                    r'("(?:[^"\\]|\\.)*")',
                    lambda m: m.group(0).replace('\n', ' ').replace('\r', '').replace('\t', ' '),
                    s,
                )
                return _json.loads(fixed)
            except Exception:
                return None

    # Find outermost JSON object
    depth = 0
    start = -1
    for i, ch in enumerate(clean):
        if ch == "{":
            if start < 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                candidate = clean[start: i + 1]
                result = _try_parse(candidate)
                if result is not None:
                    return result
                # Reset and keep searching
                start = -1
                depth = 0

    # Fallback: JSON was truncated — try to recover completed horizon objects.
    # Strategy: find the outermost "{" and greedily extract completed "key": {...} entries.
    if start >= 0:
        partial = clean[start:]
        recovered: dict = {}
        # Match completed sub-objects like "1d": {...} where the inner object is balanced.
        horizon_pattern = _re.compile(r'"(\d+d)"\s*:\s*(\{)', flags=_re.DOTALL)
        for m in horizon_pattern.finditer(partial):
            key = m.group(1)
            obj_start = m.start(2)
            d2, s2 = 0, -1
            for j, c in enumerate(partial[obj_start:], obj_start):
                if c == "{":
                    if s2 < 0:
                        s2 = j
                    d2 += 1
                elif c == "}":
                    d2 -= 1
                    if d2 == 0 and s2 >= 0:
                        obj_str = partial[s2: j + 1]
                        parsed_obj = _try_parse(obj_str)
                        if parsed_obj is not None:
                            recovered[key] = parsed_obj
                        break
        if recovered:
            return recovered
    return {}


def build_forecast_prompt(
    stock_code: str,
    market_snapshot: dict,
    market_features: dict,
    tdx_local: dict,
    capital_dims: dict,
    social_items: list[dict],
    news_items: list[dict],
    policy_items: list[dict],
    backtest_summary: dict | None = None,
) -> str:
    """Build the LLM prompt for multi-horizon price direction forecast (1-90 days).

    Provides the model with rich context: full technical picture, capital flows,
    recent news, and the historical backtest accuracy so the model can calibrate
    its own confidence. Asks for a structured JSON output.
    """
    last = market_snapshot.get("last_price")
    name = market_snapshot.get("name") or stock_code
    pct = market_snapshot.get("pct_change")

    feat = (tdx_local or {}).get("features", {})
    mf = (market_features or {}).get("features", {})

    # --- 价格与动量 ---
    series = (tdx_local or {}).get("series") or []
    closes = [float(x["close"]) for x in series if x.get("close") is not None]
    dates = [str(x.get("date") or "") for x in series]
    recent60 = closes[-61:] if len(closes) >= 61 else closes
    price_str = " ".join(f"{p:.2f}" for p in recent60[-20:])  # 最近20日收盘
    ret5 = feat.get("ret5") or mf.get("ret5") or 0.0
    ret20 = feat.get("ret20") or mf.get("ret20") or 0.0
    ret60 = (closes[-1] - closes[-61]) / closes[-61] if len(closes) >= 61 and closes[-61] else None

    # --- 技术指标 ---
    fs_series = tdx_local.get("features_series") if tdx_local else None
    fs_now = (fs_series[-1] if fs_series else {}) or feat
    rsi14 = fs_now.get("rsi14") or feat.get("rsi14") or 50.0
    macd_val = feat.get("macd") or {}
    dif = macd_val.get("dif")
    dea = macd_val.get("dea")
    macd_desc = "金叉（偏多）" if (isinstance(dif, float) and isinstance(dea, float) and dif > dea) else "死叉（偏空）"
    kdj = feat.get("kdj") or {}
    k, d_val, j = kdj.get("k"), kdj.get("d"), kdj.get("j")
    kdj_desc = ""
    if isinstance(k, float) and isinstance(j, float):
        if j > 80:
            kdj_desc = f"KDJ超买(J={j:.0f})"
        elif j < 20:
            kdj_desc = f"KDJ超卖(J={j:.0f})"
        else:
            kdj_desc = f"KDJ常态(J={j:.0f})"
    boll = feat.get("boll") or {}
    bu, bm, bl = boll.get("upper"), boll.get("mid"), boll.get("lower")
    boll_desc = ""
    if isinstance(last, float) and isinstance(bu, float) and isinstance(bl, float):
        if last > bu:
            boll_desc = f"突破布林上轨({bu:.2f})"
        elif last < bl:
            boll_desc = f"跌破布林下轨({bl:.2f})"
        else:
            pos = (last - bl) / (bu - bl) if (bu - bl) else 0.5
            boll_desc = f"布林中轨运行({pos:.0%}位置)"
    ma5 = mf.get("ma5") or feat.get("ma5")
    ma20 = mf.get("ma20") or feat.get("ma20")
    ma_desc = ""
    if isinstance(ma5, float) and isinstance(ma20, float):
        ma_desc = f"MA5({ma5:.2f}){'>' if ma5 > ma20 else '<'}MA20({ma20:.2f})，{'多头' if ma5 > ma20 else '空头'}排列"
    vol_ratio = feat.get("volume_ratio20") or fs_now.get("vol_ratio") or 1.0

    # --- 资金流 ---
    cap = (capital_dims or {}).get("capital_flow") or {}
    mforce = cap.get("main_force") or {}
    net5 = mforce.get("net_inflow_5d")
    margin = (capital_dims or {}).get("margin_financing") or {}
    # margin_financing uses "latest_rzye" for financing balance (in yuan)
    margin_bal = margin.get("latest_rzye")
    lhb = (capital_dims or {}).get("dragon_tiger") or {}
    lhb_cnt = lhb.get("lhb_count_30d", 0)

    # --- 消息面 ---
    news_titles = [x.get("title") for x in news_items[:5] if x.get("title")]
    policy_titles = [x.get("title") for x in policy_items[:3] if x.get("title")]
    social_titles = [x.get("title") for x in social_items[:3] if x.get("title")]

    # --- 历史回测准确率（近3月，技术指标信号） ---
    bt_note = ""
    if backtest_summary:
        overall_3m = backtest_summary.get("overall_accuracy")
        samples_3m = backtest_summary.get("samples")
        start_dt = backtest_summary.get("start_date", "")
        end_dt = backtest_summary.get("end_date", "")
        if overall_3m is not None:
            bt_note = f"近3月技术信号回测准确率：{overall_3m:.1%}（{samples_3m}个样本，{start_dt}~{end_dt}）。"

    prompt = (
        f"你是专业A股研报分析师，请对{name}（{stock_code}）做详细的多周期趋势预测。\n\n"
        f"【当前行情】收盘价：{last}，今日涨跌：{pct if pct is not None else 'N/A'}%\n"
        f"近5日涨跌：{ret5:+.2%}，近20日：{ret20:+.2%}"
        + (f"，近60日：{ret60:+.2%}" if ret60 is not None else "") + "\n"
        f"近20日收盘（旧→新）：{price_str}\n\n"
        f"【技术指标】\n"
        f"- 均线：{ma_desc or 'N/A'}\n"
        f"- RSI14：{rsi14:.1f}（>70超买，<30超卖）\n"
        f"- MACD：{macd_desc}\n"
        f"- {kdj_desc or 'KDJ: N/A'}\n"
        f"- {boll_desc or 'BOLL: N/A'}\n"
        f"- 量比（vs20日均量）：{vol_ratio:.2f}x\n\n"
        f"【资金面】\n"
        f"- 主力5日净流入：{'暂无' if net5 is None else _fmt_yuan(net5)}\n"
        f"- 融资余额（最新）：{'暂无' if not margin_bal else _fmt_yuan(margin_bal)}\n"
        f"- 融资余额5日变化：{'暂无' if margin.get('rzye_delta_5d') is None else _fmt_yuan(margin.get('rzye_delta_5d'))}\n"
        f"- 近30日龙虎榜上榜次数：{lhb_cnt}\n\n"
        f"【消息面】\n"
        f"- 近期新闻：{'；'.join(news_titles) or '无'}\n"
        f"- 政策动向：{'；'.join(policy_titles) or '无'}\n"
        f"- 市场热议：{'；'.join(social_titles) or '无'}\n\n"
        + (f"【参考准确率】{bt_note}\n\n" if bt_note else "")
        + "请对以下6个预测周期逐一分析，给出方向判断和核心依据。"
        "注意：不同周期可以有不同结论，短期震荡不代表中期趋势，要结合各周期特征分别判断。\n\n"
        "请严格按以下JSON格式输出，不要输出任何其他文字，所有字段值用中文：\n"
        '{"1d":{"direction":"上涨或下跌或震荡","pct_range":"如+0.5%~+1.5%","reason":"核心依据（1-2句）","confidence":"高或中或低"},'
        '"7d":{"direction":"上涨或下跌或震荡","pct_range":"如-2%~+1%","reason":"核心依据","confidence":"高或中或低"},'
        '"14d":{"direction":"上涨或下跌或震荡","pct_range":"如±3%","reason":"核心依据","confidence":"高或中或低"},'
        '"30d":{"direction":"上涨或下跌或震荡","pct_range":"如+5%~+10%","reason":"核心依据","confidence":"高或中或低"},'
        '"60d":{"direction":"上涨或下跌或震荡","pct_range":"如+10%~+20%","reason":"核心依据","confidence":"高或中或低"},'
        '"90d":{"direction":"上涨或下跌或震荡","pct_range":"如-10%~+5%","reason":"核心依据","confidence":"高或中或低"}}'
    )
    return prompt


def _forecast_readiness(backtest: dict, social_items: list[dict], policy_count: int, tdx_local: dict) -> dict:
    hs = backtest.get("horizons_recent_3m") or backtest.get("horizons") or []
    valid = [x for x in hs if x.get("accuracy") is not None and x.get("samples", 0) >= 30]
    has_backtest = len(valid) > 0
    avg_acc = sum(x["accuracy"] for x in valid) / len(valid) if valid else 0.0
    h7 = next((x for x in hs if x.get("horizon_days") == 7), None)
    h7_acc = h7.get("accuracy") if h7 else None
    tdx_ok = 1.0 if (tdx_local or {}).get("status") == "ok" else 0.0
    social_ok = 1.0 if len(social_items) > 0 else 0.6
    policy_ok = 1.0 if policy_count >= 3 else 0.7
    score = round((avg_acc * 0.45 + (h7_acc or 0.0) * 0.15 + tdx_ok * 0.2 + social_ok * 0.1 + policy_ok * 0.1) * 100, 2) if has_backtest else round((tdx_ok * 0.5 + social_ok * 0.3 + policy_ok * 0.2) * 100, 2)
    reasons = []
    if has_backtest:
        if avg_acc < 0.5:
            reasons.append("近3个月回测准确率偏低")
        if h7_acc is not None and h7_acc < 0.55:
            reasons.append("7天窗口准确率未达到建议下限")
        if len(valid) < 3:
            reasons.append("回测有效窗口不足")
    if len(social_items) == 0:
        reasons.append("相关热搜缺失，情绪因子降级")
    if policy_count < 3:
        reasons.append("政策样本偏少")
    # Stability Grading（技术信号回测已废弃，无回测数据时默认 C）
    min_s = settings.forecast_min_samples
    min_c = settings.forecast_min_coverage
    h7_s = h7.get("samples", 0) if h7 else 0
    h7_c = h7.get("coverage", 0.0) if h7 else 0.0
    if has_backtest and h7_s >= min_s * 1.5 and h7_c >= min_c * 1.5:
        grade = "A"
        grade_desc = "稳定性高(A级)"
    elif has_backtest and h7_s >= min_s and h7_c >= min_c:
        grade = "B"
        grade_desc = "稳定性中(B级)"
    else:
        grade = "C"
        grade_desc = "稳定性不足(C级)" if has_backtest else "无回测数据(C级)"

    ready = grade in ("A", "B") and score >= 60
    return {
        "score": score,
        "ready_for_use": ready,
        "reasons": reasons,
        "stability_grade": grade,
        "stability_desc": grade_desc,
        "target_ranges": {
            "horizon_7d_reasonable": "55%~65%",
            "horizon_7d_challenging": "65%~72%",
            "horizon_7d_unrealistic": ">=95%（不作为工程目标）",
        },
    }


def _forecast_actions(readiness: dict) -> list[str]:
    reasons = readiness.get("reasons") or []
    actions = []
    if any("回测准确率偏低" in r for r in reasons):
        actions.append("优先用近3个月样本重估参数，先优化7/14/30/60天窗口方向准确率。")
    if any("7天窗口准确率未达到建议下限" in r for r in reasons):
        actions.append("7天窗口优先启用‘按窗口择优模型’，并每周重算模型排名。")
    if any("置信度与历史准确率偏差较大" in r for r in reasons):
        actions.append("执行置信度校准：提高历史准确率权重，降低原始置信度对最终输出的影响。")
    if any("热搜缺失" in r for r in reasons):
        actions.append("检查热搜采集链路与关键词映射，补足股票相关社媒样本。")
    if any("政策样本偏少" in r for r in reasons):
        actions.append("补充政策源抓取与重试策略，确保政策样本>=3。")
    if not actions:
        actions.append("当前预测链路稳定，可进入灰度发布。")
    return actions


def _historical_position_view(tdx_local: dict, last_price: float | None) -> dict:
    lf = (tdx_local or {}).get("features", {})
    p60 = lf.get("price_percentile_60d")
    p252 = lf.get("price_percentile_252d")
    p3y = lf.get("price_percentile_3y")
    p5y = lf.get("price_percentile_5y")
    pall = lf.get("price_percentile_all")
    return {
        "latest_price": last_price,
        "label": lf.get("historical_position_label"),
        "percentile_60d": round(p60, 4) if isinstance(p60, (int, float)) else None,
        "percentile_252d": round(p252, 4) if isinstance(p252, (int, float)) else None,
        "percentile_3y": round(p3y, 4) if isinstance(p3y, (int, float)) else None,
        "percentile_5y": round(p5y, 4) if isinstance(p5y, (int, float)) else None,
        "percentile_all": round(pall, 4) if isinstance(pall, (int, float)) else None,
        "extreme_60d": lf.get("window_extreme_60d"),
        "extreme_252d": lf.get("window_extreme_252d"),
        "extreme_3y": lf.get("window_extreme_3y"),
        "extreme_5y": lf.get("window_extreme_5y"),
        "extreme_all": lf.get("window_extreme_all"),
    }


def _multi_cycle_alignment(market_features: dict, tdx_local: dict) -> dict:
    f = (market_features or {}).get("features") or {}
    lf = (tdx_local or {}).get("features") or {}
    ma5 = f.get("ma5")
    ma20 = f.get("ma20")
    ret60 = lf.get("ret60")
    ret120 = lf.get("ret120")
    ret252 = lf.get("ret252")
    short = "偏多" if isinstance(ma5, (int, float)) and isinstance(ma20, (int, float)) and ma5 >= ma20 else "偏空"
    medium = "偏多" if isinstance(ret60, (int, float)) and ret60 >= 0 else "偏空"
    long = "偏多" if isinstance(ret252, (int, float)) and ret252 >= 0 else "偏空"
    agree = len({short, medium, long}) == 1
    regime = "趋势" if agree else "震荡"
    vol20 = lf.get("volatility20")
    if isinstance(vol20, (int, float)) and vol20 > 0.05:
        regime = "高波动"
    return {
        "regime_tag": regime,
        "short_cycle": short,
        "medium_cycle": medium,
        "long_cycle": long,
        "agree_all": agree,
        "ret120": ret120,
    }


def _calibrate_confidence(raw_conf: float, empirical_acc: float | None, samples: int | None) -> tuple[float, dict]:
    if empirical_acc is None or samples is None or samples <= 0:
        c = round(max(0.32, min(0.86, raw_conf * 0.92)), 3)
        return c, {"mode": "raw_only", "reliability": 0.0}
    reliability = max(0.0, min(1.0, samples / 120.0))
    alpha = 0.25 + 0.45 * reliability
    calibrated = raw_conf * (1.0 - alpha) + empirical_acc * alpha
    calibrated = max(0.32, min(0.86, calibrated))
    return round(calibrated, 3), {"mode": "empirical_blend", "reliability": round(reliability, 3), "alpha": round(alpha, 3)}


def _nearest_model_for_day(day: int, selected_map: dict[int, dict]) -> str:
    if day in selected_map and selected_map[day].get("model_name"):
        return selected_map[day]["model_name"]
    keys = [k for k in selected_map.keys() if isinstance(k, int)]
    if not keys:
        return "动量回归混合"
    k = min(keys, key=lambda x: abs(x - day))
    return selected_map.get(k, {}).get("model_name", "动量回归混合")


def _apply_direction_override(
    base_recommendation: str,
    direction_forecast: dict,
) -> tuple[str, dict]:
    d7 = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 7), None)
    d7b = next((x for x in (direction_forecast.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), None)
    min_samples = max(1, int(settings.forecast_min_samples))
    min_coverage = float(settings.forecast_min_coverage)
    samples = int((d7b or {}).get("actionable_samples") or 0)
    coverage = float((d7b or {}).get("actionable_coverage") or 0.0)
    acc_7 = float((d7b or {}).get("actionable_accuracy") or 0.0)
    d7_reliable = bool(d7b and samples >= min_samples and coverage >= min_coverage)
    # 样本不足时不得直接给出强卖出（数据不可靠）
    if base_recommendation == "SELL":
        if samples < 5 or coverage < 0.05:
            return "HOLD", {
                "d7_reliable": d7_reliable,
                "reason": "stability_guard_strong_sell",
                "samples": samples,
                "coverage": coverage,
                "min_samples": min_samples,
                "min_coverage": min_coverage,
            }
    if d7_reliable and d7:
        action = d7.get("action")
        if action in ("BUY", "SELL"):
            return action, {
                "d7_reliable": True,
                "reason": f"override_to_{action.lower()}",
                "samples": samples,
                "coverage": coverage,
                "min_samples": min_samples,
                "min_coverage": min_coverage,
            }
    return base_recommendation, {
        "d7_reliable": d7_reliable,
        "reason": "keep_base",
        "samples": samples,
        "coverage": coverage,
        "min_samples": min_samples,
        "min_coverage": min_coverage,
    }


def _build_direction_forecast(
    closes: list[float],
    selected_map: dict[int, dict],
    candidate_models: list[str] | None = None,
    features_series: list[dict] | None = None,
) -> dict:
    # 模板：未来第几日表用 1~7 日；主要依据表用 1/7/14/30/60 日
    DAYS_FIRST_TABLE = [1, 2, 3, 4, 5, 6, 7]
    DAYS_MAIN_BASIS = [1, 7, 14, 30, 60]
    days_all = list(dict.fromkeys(DAYS_FIRST_TABLE + DAYS_MAIN_BASIS))  # 1,2,3,4,5,6,7,14,30,60
    if len(closes) < 60:
        return {
            "horizons": [],
            "backtest_recent_3m": [],
            "target": {"target_accuracy": settings.forecast_target_accuracy, "min_actionable_samples": 1, "min_actionable_coverage": 0.01},
            "summary": {"target_met_days": [], "target_failed_days": DAYS_MAIN_BASIS, "note": "历史样本不足"},
        }

    fs = features_series
    if fs and len(fs) != len(closes):
        # Align to end: features_series may be built from full history while closes may be trimmed.
        offset = len(fs) - len(closes)
        if offset > 0:
            fs = fs[offset:]
        elif offset < 0:
            fs = None

    train_end = max(min(120, max(30, len(closes) - 30)), len(closes) - 63)
    # Split recent 3m into train (2/3) and validation (1/3) to prevent data leakage
    recent_len = len(closes) - train_end
    val_split = train_end + max(1, int(recent_len * 2 / 3))  # train on first 2/3, validate on last 1/3
    days = days_all
    # Dynamic thresholds based on recent volatility
    rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(max(1, len(closes)-20), len(closes)) if closes[i-1] != 0]
    vol20 = (sum(r**2 for r in rets) / max(1, len(rets))) ** 0.5 if rets else 0.02
    thresholds = sorted(set(round(vol20 * k, 5) for k in [0.15, 0.2, 0.3, 0.4, 0.5, 0.7, 1.0, 1.3, 1.6, 2.0]))
    candidate_models = candidate_models or []
    out = []
    bt = []
    target_accuracy = settings.forecast_target_accuracy
    min_actionable_samples = settings.forecast_min_samples
    min_actionable_coverage = settings.forecast_min_coverage
    met_days = []
    failed_days = []

    for d in days:
        model_pool = candidate_models[:] if candidate_models else [_nearest_model_for_day(d, selected_map)]
        best = None
        # threshold tuning on TRAIN portion of recent 3m (first 2/3)
        for model_name in model_pool:
            for th in thresholds:
                act_n = 0
                hit_n = 0
                total_n = 0
                for i in range(train_end, min(val_split, len(closes) - d)):
                    sig = _signal_value(model_name, closes, i, features_series=fs, horizon_days=d)
                    if sig == 0:
                        continue
                    total_n += 1
                    if abs(sig) < th:
                        continue
                    act_n += 1
                    actual = (closes[i + d] - closes[i]) / closes[i] if closes[i] else 0.0
                    if (sig > 0 and actual > 0) or (sig < 0 and actual < 0):
                        hit_n += 1
                if act_n < 1:
                    continue
                acc = hit_n / act_n
                cov = act_n / max(1, total_n)
                cand = {"threshold": th, "accuracy": acc, "coverage": cov, "samples": act_n, "model_name": model_name}
                if best is None:
                    best = cand
                    continue
                # prefer target-meeting threshold with higher samples/coverage
                cur_meet = (
                    best["accuracy"] >= target_accuracy
                    and best["samples"] >= min_actionable_samples
                    and best["coverage"] >= min_actionable_coverage
                )
                new_meet = acc >= target_accuracy and act_n >= min_actionable_samples and cov >= min_actionable_coverage
                if new_meet and not cur_meet:
                    best = cand
                elif new_meet and cur_meet and (act_n > best["samples"] or (act_n == best["samples"] and cov > best["coverage"])):
                    best = cand
                elif (not cur_meet) and (not new_meet):
                    if acc > best["accuracy"] or (acc == best["accuracy"] and cov > best["coverage"]):
                        best = cand

        if best is None:
            best = {"threshold": 0.01, "accuracy": 0.0, "coverage": 0.0, "samples": 0, "model_name": _nearest_model_for_day(d, selected_map)}
        model_name = best["model_name"]

        # evaluate on VALIDATION portion (last 1/3) — unseen during threshold tuning
        act_n = 0
        hit_n = 0
        total_n = 0
        for i in range(val_split, len(closes) - d):
            sig = _signal_value(model_name, closes, i, features_series=fs, horizon_days=d)
            if sig == 0:
                continue
            total_n += 1
            if abs(sig) < best["threshold"]:
                continue
            act_n += 1
            actual = (closes[i + d] - closes[i]) / closes[i] if closes[i] else 0.0
            if (sig > 0 and actual > 0) or (sig < 0 and actual < 0):
                hit_n += 1
        acc_recent = (hit_n / act_n) if act_n > 0 else None
        cov_recent = (act_n / max(1, total_n)) if total_n > 0 else 0.0
        target_met = bool(
            acc_recent is not None
            and acc_recent >= target_accuracy
            and act_n >= min_actionable_samples
            and cov_recent >= min_actionable_coverage
        )
        if target_met:
            met_days.append(d)
        else:
            failed_days.append(d)

        # current decision
        sig_now = _signal_value(model_name, closes, len(closes) - 1, features_series=fs, horizon_days=d)
        if abs(sig_now) < best["threshold"]:
            direction = "震荡"
            action = "HOLD"
        elif sig_now > 0:
            direction = "上涨"
            action = "BUY"
        else:
            direction = "下跌"
            action = "SELL"
        conf_raw = max(0.35, min(0.86, 0.5 + min(0.3, abs(sig_now) / max(best["threshold"], 1e-6) * 0.08)))
        conf, _ = _calibrate_confidence(conf_raw, acc_recent, act_n)

        out.append(
            {
                "horizon_day": d,
                "direction": direction,
                "action": action,
                "confidence": conf,
                "signal_value": round(sig_now, 6),
                "threshold": best["threshold"],
                "selected_signal_model": model_name,
                "target_met": target_met,
            }
        )
        # 仅 1/7/14/30/60 日写入主要依据表
        if d in DAYS_MAIN_BASIS:
            bt.append(
                {
                    "horizon_day": d,
                    "actionable_accuracy": round(acc_recent, 4) if acc_recent is not None else None,
                    "actionable_samples": act_n,
                    "actionable_coverage": round(cov_recent, 4),
                    "total_samples": total_n,
                }
            )

    d7_row = next((x for x in bt if x.get("horizon_day") == 7), None)
    reliability_warning = None
    if d7_row and ((d7_row.get("actionable_samples", 0) < 5) or (d7_row.get("actionable_coverage", 0.0) < 0.05)):
        reliability_warning = "7天信号样本或覆盖率偏低，建议谨慎使用卖出结论。"

    return {
        "horizons": out,
        "backtest_recent_3m": bt,
        "target": {
            "target_accuracy": target_accuracy,
            "min_actionable_samples": min_actionable_samples,
            "min_actionable_coverage": min_actionable_coverage,
        },
        "summary": {
            "target_met_days": met_days,
            "target_failed_days": failed_days,
            "note": "目标按可操作信号（持有/卖出）统计，采用最近3个月窗口自适应阈值，含样本与覆盖率约束。",
            "reliability_warning": reliability_warning,
        },
    }


async def _calc_price_forecast(
    market_snapshot: dict,
    market_features: dict,
    social_items: list[dict],
    tdx_local: dict,
    market_breadth: dict,
    capital_dims: dict | None = None,
    policy_count: int = 0,
    stock_code: str = "",
    news_items: list[dict] | None = None,
    policy_items: list[dict] | None = None,
) -> dict:
    last = market_snapshot.get("last_price")
    f = market_features.get("features", {})
    if last is None:
        return {
            "windows": [],
            "method": "unavailable",
            "note": "实时行情不可用，无法计算预测价格",
        }

    ret5 = f.get("ret5") or 0.0
    ret20 = f.get("ret20") or 0.0
    trend = f.get("trend") or "震荡"
    trend_adj = 0.0008 if trend == "偏多" else (-0.0008 if trend == "偏空" else 0.0)
    social_signal = 0.0
    if social_items:
        social_signal = sum((x.get("sentiment_score", 0.0) or 0.0) * (x.get("relevance_score", 0.0) or 0.0) for x in social_items) / max(
            1, len(social_items)
        )

    lf = (tdx_local or {}).get("features", {})
    momentum_score = _factor_score((lf.get("ret5") if lf.get("ret5") is not None else ret5), 0.06)
    ma20_base = f.get("ma20") or last or 0.0
    mean_reversion_raw = -(((last or 0.0) - ma20_base) / ma20_base) if ma20_base else 0.0
    mean_reversion_score = _factor_score(mean_reversion_raw, 0.05)
    trend_score = _factor_score((f.get("ma5", 0) - f.get("ma20", 0)) / (f.get("ma20", 1) or 1), 0.05)
    volatility_penalty = -abs(_factor_score(lf.get("volatility20", 0.0), 0.04))
    liquidity_score = _factor_score((lf.get("volume_ratio20", 1.0) - 1.0), 0.8)
    social_score = _factor_score(social_signal, 0.6)
    breadth_score = _factor_score((market_breadth or {}).get("breadth_score", 0.0), 0.25)
    capital_flow = (capital_dims or {}).get("capital_flow") or {}
    main_force = capital_flow.get("main_force") or {}
    dragon_tiger = (capital_dims or {}).get("dragon_tiger") or {}
    mf5 = main_force.get("net_inflow_5d")
    lhb_ratio = dragon_tiger.get("avg_net_buy_ratio")
    capital_score = _factor_score(mf5 if isinstance(mf5, (int, float)) else 0.0, 2e8)
    lhb_score = _factor_score((lhb_ratio if isinstance(lhb_ratio, (int, float)) else 0.0), 0.08)

    factors = [
        {"factor": "momentum", "factor_cn": "短期动量", "score": momentum_score, "weight": 0.20},
        {"factor": "mean_reversion", "factor_cn": "均值回归", "score": mean_reversion_score, "weight": 0.16},
        {"factor": "trend", "factor_cn": "均线趋势", "score": trend_score, "weight": 0.14},
        {"factor": "volatility_penalty", "factor_cn": "波动惩罚", "score": volatility_penalty, "weight": 0.12},
        {"factor": "liquidity", "factor_cn": "量能", "score": liquidity_score, "weight": 0.08},
        {"factor": "social", "factor_cn": "情绪热度", "score": social_score, "weight": 0.05},
        {"factor": "market_breadth", "factor_cn": "市场宽度", "score": breadth_score, "weight": 0.10},
        {"factor": "main_force_flow", "factor_cn": "主力资金流", "score": capital_score, "weight": 0.10},
        {"factor": "dragon_tiger", "factor_cn": "龙虎榜净买特征", "score": lhb_score, "weight": 0.05},
    ]
    base_score = sum(x["score"] * x["weight"] for x in factors)
    daily_drift_base = max(-0.025, min(0.025, base_score * 0.01 + trend_adj + ret5 * 0.02))

    windows = [1, 7, 14, 30, 60]
    out = []
    horizon_logic = []
    series = (tdx_local or {}).get("series") or []
    closes = [float(x.get("close")) for x in series if x.get("close") is not None]
    # Pre-compute features_series once; pass it to backtest to avoid duplicate computation.
    fs_now = build_features_series(stock_code) if stock_code else []
    backtest = _backtest_forecast_model(tdx_local, stock_code=stock_code, features_series=fs_now or None)
    selected_map = {int(x["horizon_days"]): x for x in (backtest.get("selected_model_by_horizon") or [])}
    candidate_models = [x.get("model_name") for x in (backtest.get("model_candidates") or []) if x.get("model_name")]
    direction_forecast = _build_direction_forecast(
        closes,
        selected_map,
        candidate_models=candidate_models,
        features_series=fs_now or None,
    )
    for d in windows:
        selected_model = selected_map.get(d, {}).get("model_name", "动量回归混合")
        signal_now = _signal_value(selected_model, closes, len(closes) - 1, features_series=fs_now or None) if len(closes) > 30 else 0.0
        signal_drift = max(-0.02, min(0.02, signal_now * 0.35))
        daily_drift = max(-0.03, min(0.03, daily_drift_base * 0.55 + signal_drift * 0.45))
        horizon_scale = d ** 0.82
        risk_adj = max(0.6, 1.0 - abs(lf.get("volatility20", 0.0)) * (d / 60.0))
        pred_ret = round(daily_drift * horizon_scale * risk_adj, 6)
        pred_ret = max(-0.35, min(0.35, pred_ret))
        pred_price = round(float(last) * (1 + pred_ret), 2)
        confidence_raw = round(max(0.50, 0.84 - (d / 130.0) - abs(lf.get("volatility20", 0.0)) * 0.6), 3)
        empirical_acc = selected_map.get(d, {}).get("accuracy_used")
        empirical_samples = selected_map.get(d, {}).get("samples_used")
        confidence, calib_meta = _calibrate_confidence(confidence_raw, empirical_acc, empirical_samples)
        confidence_gap = round(abs(confidence - empirical_acc), 3) if isinstance(empirical_acc, (int, float)) else None
        out.append(
            {
                "horizon_days": d,
                "predicted_price": pred_price,
                "predicted_return": pred_ret,
                "confidence": confidence,
                "confidence_raw": confidence_raw,
                "confidence_empirical_accuracy": empirical_acc,
                "confidence_gap": confidence_gap,
                "confidence_sample_scope": selected_map.get(d, {}).get("sample_scope"),
                "method": "多因子动量法",
                "selected_signal_model": selected_model,
            }
        )
        horizon_logic.append(
            {
                "horizon_days": d,
                "selected_signal_model": selected_model,
                "signal_now": round(signal_now, 6),
                "horizon_scale": round(horizon_scale, 4),
                "risk_adjust": round(risk_adj, 4),
                "confidence_calibration": calib_meta,
            }
        )

    readiness = _forecast_readiness(backtest, social_items=social_items, policy_count=policy_count, tdx_local=tdx_local)
    valid_gaps = [x.get("confidence_gap") for x in out if isinstance(x.get("confidence_gap"), (int, float))]
    avg_gap = round(sum(valid_gaps) / len(valid_gaps), 3) if valid_gaps else None
    if isinstance(avg_gap, float) and avg_gap > 0.12:
        readiness.setdefault("reasons", []).append("置信度与历史准确率偏差较大")
        readiness["ready_for_use"] = False
    actions = _forecast_actions(readiness)
    historical_position = _historical_position_view(tdx_local, last)
    cycle_view = _multi_cycle_alignment(market_features, tdx_local)

    # --- LLM 多周期方向预测 ---
    # Build forecast prompt with full technical + capital + news context.
    bt_sum_3m = backtest.get("summary_recent_3m") or {}
    forecast_prompt = build_forecast_prompt(
        stock_code=stock_code,
        market_snapshot=market_snapshot,
        market_features=market_features,
        tdx_local=tdx_local,
        capital_dims=capital_dims or {},
        social_items=social_items,
        news_items=news_items or [],
        policy_items=policy_items or [],
        backtest_summary=bt_sum_3m if bt_sum_3m.get("overall_accuracy") is not None else None,
    )
    llm_forecast_raw: dict = {}
    llm_forecast_error: str | None = None
    llm_raw_response: str = ""
    try:
        # deepseek-r1 generates chain-of-thought; allow up to 120s for multi-horizon analysis
        llm_resp = await ollama_client.generate(
            prompt=forecast_prompt, use_prod=False, timeout=120
        )
        llm_raw_response = (llm_resp.get("response") or "").strip()
        llm_forecast_raw = _parse_llm_forecast(llm_raw_response)
        if not llm_forecast_raw:
            llm_forecast_error = f"parse_failed: {llm_raw_response[:200]}"
    except Exception as _e:
        llm_forecast_error = str(_e)[:200]
        llm_forecast_raw = {}

    # Map LLM output horizon labels to days
    _horizon_label_map = {"1d": 1, "7d": 7, "14d": 14, "30d": 30, "60d": 60, "90d": 90}
    # LLM confidence labels → numeric confidence (0-1).
    # 校准说明：最高"高"=0.72（低于旧版0.78），确保strong BUY信号能达到开仓门槛0.65。
    # 开仓门槛已从0.85下调至0.65（见 should_generate_instruction）；两处须同步维护。
    _conf_map = {"高": 0.72, "中高": 0.65, "中": 0.55, "中低": 0.47, "低": 0.38}

    def _llm_dir_to_action(direction: str) -> str:
        """Map LLM direction string (may be richer than 3 values) to BUY/SELL/HOLD."""
        d = (direction or "").strip()
        if not d:
            return "HOLD"
        if "下跌" in d or "下行" in d or "偏空" in d or "看空" in d:
            return "SELL"
        if "上涨" in d or "上行" in d or "偏强" in d or "偏多" in d or "看多" in d:
            return "BUY"
        return "HOLD"  # 震荡 / 中性 / etc.

    # Build per-horizon backtest accuracy lookup (from _backtest_forecast_model horizons_recent_3m)
    # This gives the best technical signal's accuracy for each horizon, useful as a reference baseline.
    bt_h3m_lookup: dict[int, dict] = {}
    for h_row in (backtest.get("horizons_recent_3m") or []):
        hd = int(h_row.get("horizon_days") or 0)
        if hd > 0:
            bt_h3m_lookup[hd] = h_row

    # Merge LLM forecasts into windows list — fixed 5 windows: 1/7/14/30/60 (契约§6条款5)
    # 90d 仅用于 LLM prompt 背景参考，不作为正式输出窗口
    windows_contract = [1, 7, 14, 30, 60]
    out_map = {w["horizon_days"]: w for w in out}
    merged_windows = []
    for d in windows_contract:
        label = f"{d}d"
        llm_item = llm_forecast_raw.get(label) or {}
        llm_dir = llm_item.get("direction") or "震荡"
        llm_pct_range = llm_item.get("pct_range") or ""
        llm_reason = llm_item.get("reason") or ""
        llm_conf_str = llm_item.get("confidence") or "中"
        llm_conf = _conf_map.get(llm_conf_str, 0.65)
        llm_action = _llm_dir_to_action(llm_dir)

        base = out_map.get(d, {})
        # confidence_raw 来自多因子公式（契约§6条款24必填）
        confidence_raw = base.get("confidence_raw")
        empirical_acc = base.get("confidence_empirical_accuracy")
        # confidence_gap = |LLM校准置信度 - 历史回测准确率|（契约§6条款24）
        confidence_gap = round(abs(llm_conf - empirical_acc), 3) if isinstance(empirical_acc, (int, float)) else None

        # Per-horizon technical signal backtest accuracy (for reference display only).
        # Suppress accuracy when sample count < 10 to prevent misleading statistics.
        bt_h = bt_h3m_lookup.get(d) or {}
        bt_accuracy = bt_h.get("accuracy")
        bt_samples = bt_h.get("samples")
        bt_sample_scope = bt_h.get("sample_scope") or "近3个月"
        if bt_samples is not None and bt_samples < 10:
            bt_accuracy = None   # insufficient samples; do not display to avoid false precision

        entry = {
            "horizon_days": d,
            # LLM primary outputs
            "llm_direction": llm_dir,
            "llm_action": llm_action,
            "llm_pct_range": llm_pct_range,
            "llm_reason": llm_reason,
            "llm_confidence_label": llm_conf_str,
            "llm_confidence": llm_conf,
            # Per-horizon technical signal backtest (reference baseline only)
            "bt_accuracy": bt_accuracy,
            "bt_samples": bt_samples,
            "bt_sample_scope": bt_sample_scope,
            # Formula-derived reference values (kept for backward compat)
            "predicted_price": base.get("predicted_price"),
            "predicted_return": base.get("predicted_return"),
            # 契约§6条款24：confidence_raw/confidence_empirical_accuracy/confidence_gap 必填
            "confidence": llm_conf,
            "confidence_raw": confidence_raw,
            "confidence_empirical_accuracy": empirical_acc,
            "confidence_gap": confidence_gap,
            "method": "LLM多周期分析" if llm_dir != "震荡" or llm_reason else "多因子动量法（LLM无效）",
            "selected_signal_model": base.get("selected_signal_model", ""),
            "source": "llm" if llm_forecast_raw else "formula",
        }
        merged_windows.append(entry)

    return {
        "windows": merged_windows,
        "method": "本地8B模型多周期方向分析",
        "direction_forecast": direction_forecast,
        "llm_forecast_raw": llm_forecast_raw,
        "llm_forecast_error": llm_forecast_error,
        "llm_prompt": forecast_prompt,
        "llm_raw_response": llm_raw_response,
        "inputs": {
            "last_price": last,
            "ret5": ret5,
            "ret20": ret20,
            "trend": trend,
            "social_signal": round(social_signal, 6),
            "daily_drift": round(daily_drift_base, 6),
        },
        "explain": {
            "factor_contributions": [
                {
                    "factor": x["factor"],
                    "factor_cn": x["factor_cn"],
                    "score": round(x["score"], 6),
                    "weight": x["weight"],
                    "contribution": round(x["score"] * x["weight"], 6),
                }
                for x in factors
            ],
            "horizon_logic": horizon_logic,
            "logic_summary": "预测方向由本地8B模型结合技术指标、资金面、消息面综合分析得出；参考价格由多因子动量公式辅助计算。",
            "historical_position": historical_position,
            "historical_position_long": {
                "percentile_3y": historical_position.get("percentile_3y"),
                "percentile_5y": historical_position.get("percentile_5y"),
                "percentile_all": historical_position.get("percentile_all"),
                "extreme_3y": historical_position.get("extreme_3y"),
                "extreme_5y": historical_position.get("extreme_5y"),
                "extreme_all": historical_position.get("extreme_all"),
            },
            "multi_cycle_alignment": cycle_view,
            "regime_tag": cycle_view.get("regime_tag"),
            "selected_model_by_horizon": backtest.get("selected_model_by_horizon", []),
            "confidence_diagnostics": {
                "avg_confidence_gap": avg_gap,
                "note": "confidence 为 LLM 判断融合后的置信度。",
            },
        },
        "backtest": backtest,
        "readiness": readiness,
        "improvement_actions": actions,
    }


def _stable_summary_cn(
    final_reco: str,
    rule_reco: str,
    llm_reco: str,
    market_snapshot: dict,
    market_features: dict,
    social_count: int,
    news_count: int,
    policy_count: int,
    llm_reason: str,
) -> str:
    reco_cn = {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(final_reco, "观望等待")
    f = market_features.get("features", {})
    conflict = "是" if (rule_reco != llm_reco) else "否"
    return (
        f"建议：{reco_cn}。"
        f"最终动作已统一，冲突状态={conflict}。"
        f"当前价={market_snapshot.get('last_price')}，趋势={f.get('trend')}，近5日收益={f.get('ret5')}。"
        f"已纳入社媒{social_count}条、新闻{news_count}条、政策{policy_count}条。"
        f"AI分析摘要：{(llm_reason[:120] + '...') if llm_reason and len(llm_reason) > 120 else (llm_reason or '无')}"
    )


def _build_novice_guide(
    stock_code: str,
    recommendation: str,
    direction_forecast: dict,
    market_snapshot: dict,
    market_features: dict,
    social_count: int,
    news_count: int,
    policy_count: int,
) -> dict:
    reco_cn = {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(recommendation, "观望等待")
    d7 = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 7), {})
    d1 = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 1), {})
    b7 = next((x for x in (direction_forecast.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), {})
    trend = (market_features.get("features") or {}).get("trend")
    ma5 = (market_features.get("features") or {}).get("ma5")
    ma20 = (market_features.get("features") or {}).get("ma20")
    coverage = b7.get("actionable_coverage")
    acc = b7.get("actionable_accuracy")
    samples = b7.get("actionable_samples")
    risk_level = "高" if recommendation == "SELL" else ("中" if recommendation == "HOLD" else "中")
    one_line = f"{stock_code} 未来1~7天以“{d7.get('direction','震荡')}”为主，当前建议：{reco_cn}。"
    why = [
        f"短期方向信号：1天={d1.get('direction','-')}，7天={d7.get('direction','-')}。",
        f"技术位置：五日均线={ma5}，二十日均线={ma20}，趋势={trend or '未知'}。",
        f"外部信息：新闻{news_count}条、政策{policy_count}条、相关热搜{social_count}条。",
    ]
    stable_tag = "稳定性较好" if ((samples or 0) >= 5 and (coverage or 0) >= 0.05) else "稳定性偏弱（样本或覆盖率不足）"
    show_stability_reminder = (samples or 0) < 5 or (coverage or 0) < 0.05  # 05：页面须展示稳定性提醒
    uncertainty = f"7天可操作准确率={acc}，样本={samples}，覆盖率={coverage}，当前判断：{stable_tag}。"
    next_watch = [
        "若连续2天收盘跌破二十日均线，继续偏保守。",
        "若成交量明显放大且价格重新站上五日均线，可从卖出转为持有观察。",
        "出现重大政策或公司公告时，需优先复核方向信号。",
    ]
    glossary = [
        {"term": "稳定性评级", "plain_explain": "基于回测样本量与覆盖率的综合评分(A/B/C)。"},
    ]
    # Inject stability grade into existing logic if needed or just return it
    # The dictionary returned here is used for the frontend "novice guide" card.
    return {
        "one_line_decision": one_line,
        "action_for_beginner": reco_cn,
        "why_points": why,
        "uncertainty": uncertainty,
        "risk_level": risk_level,
        "next_watch_points": next_watch,
        "glossary": glossary,
        "stability_reminder": "7天信号样本或覆盖率不足，建议谨慎使用强卖出结论；当前已做保守处理。" if show_stability_reminder else None,
    }


def _build_dual_market_brief(market_snapshot: dict) -> dict:
    dual_sources = market_snapshot.get("dual_sources") or {}
    east = dual_sources.get("eastmoney") or {}
    tdx = dual_sources.get("tdx") or {}
    comp = market_snapshot.get("dual_comparison") or {}
    return {
        "selected_source": market_snapshot.get("source"),
        "selected_price": market_snapshot.get("last_price"),
        "eastmoney_price": east.get("last_price"),
        "tdx_price": tdx.get("last_price"),
        "diff_abs": comp.get("last_price_diff_abs"),
        "diff_pct": comp.get("last_price_diff_pct"),
        "price_consistent": comp.get("price_consistent"),
        "dual_status": market_snapshot.get("dual_status"),
        "fetch_time_selected": market_snapshot.get("fetch_time"),
    }


def _build_accuracy_explain(direction_forecast: dict, price_forecast: dict) -> dict:
    d7 = next((x for x in (direction_forecast.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), {})
    p7 = next((x for x in (price_forecast.get("windows") or []) if x.get("horizon_days") == 7), {})
    h7_logic = next((x for x in ((price_forecast.get("explain") or {}).get("horizon_logic") or []) if x.get("horizon_days") == 7), {})
    calib = h7_logic.get("confidence_calibration") or {}
    backtest = price_forecast.get("backtest") or {}
    summary_3m = backtest.get("summary_recent_3m") or {}
    readiness = price_forecast.get("readiness") or {}
    target = direction_forecast.get("target") or {}
    target_summary = direction_forecast.get("summary") or {}

    d7_acc = d7.get("actionable_accuracy")
    d7_samples = d7.get("actionable_samples")
    d7_cov = d7.get("actionable_coverage")
    stable = bool(isinstance(d7_samples, int) and d7_samples >= 5 and isinstance(d7_cov, (int, float)) and d7_cov >= 0.05)
    headline = (
        f"7天可操作准确率={d7_acc}，样本={d7_samples}，覆盖率={d7_cov}，当前稳定性：{'可执行' if stable else '偏弱'}。"
        if d7
        else "当前缺少7天可操作回测样本，准确率结论需谨慎。"
    )

    window_board = []
    for row in (backtest.get("horizons_recent_3m") or []):
        window_board.append(
            {
                "horizon_days": row.get("horizon_days"),
                "accuracy_3m": row.get("accuracy"),
                "samples_3m": row.get("samples"),
                "coverage_3m": row.get("coverage"),
            }
        )

    return {
        "headline": headline,
        "current_metrics": {
            "horizon_7d_actionable_accuracy": d7_acc,
            "horizon_7d_samples": d7_samples,
            "horizon_7d_coverage": d7_cov,
            "backtest_overall_3m_accuracy": summary_3m.get("overall_accuracy"),
            "backtest_overall_3m_samples": summary_3m.get("samples"),
            "readiness_score": readiness.get("score"),
            "ready_for_use": readiness.get("ready_for_use"),
        },
        "confidence_formula": {
            "formula": "final_confidence = raw_confidence*(1-alpha) + empirical_accuracy*alpha",
            "raw_name": "confidence_raw",
            "empirical_name": "confidence_empirical_accuracy",
            "alpha_name": "alpha",
            "calibrated_name": "confidence",
        },
        "confidence_case_7d": {
            "raw_confidence": p7.get("confidence_raw"),
            "empirical_accuracy": p7.get("confidence_empirical_accuracy"),
            "alpha": calib.get("alpha"),
            "reliability": calib.get("reliability"),
            "calibrated_confidence": p7.get("confidence"),
            "sample_scope": p7.get("confidence_sample_scope"),
        },
        "target_explain": {
            "target_accuracy": target.get("target_accuracy"),
            "min_actionable_samples": target.get("min_actionable_samples"),
            "min_actionable_coverage": target.get("min_actionable_coverage"),
            "target_met_days": target_summary.get("target_met_days") or [],
            "target_failed_days": target_summary.get("target_failed_days") or [],
        },
        "window_accuracy_board": window_board,
        "plain_steps": [
            "第一步：先看7天可操作准确率，并同时看样本和覆盖率。",
            "第二步：再看近3个月整体回测准确率，判断模型整体状态。",
            "第三步：置信度不是直接给的，而是原始置信度和历史准确率按alpha融合。",
            "第四步：若样本或覆盖率不足，即使准确率高，也按低稳定性处理并降仓。",
        ],
    }


def _build_quality_gate(
    market_snapshot: dict,
    price_forecast: dict,
    capital_dims: dict,
    social_items: list[dict],
    news_items: list[dict],
    policy_items: list[dict],
) -> dict:
    reasons: list[str] = []
    missing_fields: list[str] = []
    recover_actions: list[str] = []
    dim_cov = {}

    dual_ok = market_snapshot.get("dual_status") in {"ok_both", "ok_eastmoney_only", "ok_tdx_only"}
    dim_cov["market_snapshot"] = 1.0 if dual_ok else 0.0
    if not dual_ok:
        reasons.append("核心行情不可用")
        missing_fields.append("market_snapshot")
        recover_actions.append("检查 eastmoney/tdx 数据源连通性并等待熔断恢复后重试")

    cap = (capital_dims or {}).get("capital_flow") or {}
    lhb = (capital_dims or {}).get("dragon_tiger") or {}
    margin = (capital_dims or {}).get("margin_financing") or {}
    dim_cov["capital_flow"] = 1.0 if cap.get("main_force", {}).get("status") in {"ok", "stale_ok", "proxy_ok"} else 0.0
    dim_cov["dragon_tiger"] = 1.0 if lhb.get("status") == "ok" else 0.0
    dim_cov["margin_financing"] = 1.0 if margin.get("status") in {"ok", "stale_ok"} else 0.0
    if dim_cov["capital_flow"] == 0.0:
        reasons.append("主力资金流维度缺失")
        missing_fields.append("capital_flow.main_force")
        recover_actions.append("检查资金接口可用性，失败时优先回填历史缓存")
    if dim_cov["dragon_tiger"] == 0.0:
        reasons.append("龙虎榜维度缺失")
        missing_fields.append("dragon_tiger")
        recover_actions.append("检查龙虎榜远端源与本地数据集")
    if dim_cov["margin_financing"] == 0.0:
        reasons.append("两融维度缺失")
        missing_fields.append("margin_financing")
        recover_actions.append("检查两融分页抓取接口并确认分页数据是否返回")

    dim_cov["social"] = 1.0 if len(social_items) > 0 else 0.0
    dim_cov["news"] = 1.0 if len(news_items) > 0 else 0.0
    dim_cov["policy"] = 1.0 if len(policy_items) > 0 else 0.0

    news_titles = [str(x.get("title") or "") for x in (news_items or [])]
    low_quality_news_count = len([t for t in news_titles if _is_low_quality_news_title(t)])
    news_quality_score = (
        round(max(0.0, 1.0 - (low_quality_news_count / max(1, len(news_titles)))), 4) if news_titles else 0.0
    )
    if news_titles and news_quality_score < 0.6:
        reasons.append("新闻质量偏低")
        missing_fields.append("news_quality")
        recover_actions.append("提升新闻过滤质量并降低低信息密度标题占比")

    readiness = (price_forecast or {}).get("readiness") or {}
    stability_grade = readiness.get("stability_grade") or "C"
    if not readiness.get("ready_for_use"):
        reasons.extend(readiness.get("reasons") or [])

    # Optional dimensions (social/policy) use lower weight to avoid over-penalizing reports.
    cov_weights = {
        "market_snapshot": 0.25,
        "capital_flow": 0.2,
        "dragon_tiger": 0.1,
        "margin_financing": 0.2,
        "news": 0.15,
        "policy": 0.05,
        "social": 0.05,
    }
    weighted_sum = 0.0
    weighted_den = 0.0
    for k, v in dim_cov.items():
        w = cov_weights.get(k, 0.1)
        weighted_sum += float(v) * w
        weighted_den += w
    avg_cov = round(weighted_sum / max(1e-9, weighted_den), 4)
    feature_quality_score = round((avg_cov * 0.7 + news_quality_score * 0.3) * 100, 2)
    if not dual_ok:
        decision = "hold"
    elif readiness.get("ready_for_use") and avg_cov >= 0.7 and feature_quality_score >= settings.quality_gate_min_score:
        decision = "publish"
    else:
        decision = "degraded"
    return {
        "publish_decision": decision,
        "degrade_reason": reasons,
        "missing_fields": sorted(set(missing_fields)),
        "recover_actions": list(dict.fromkeys(recover_actions)),
        "dimension_coverage": dim_cov,
        "stability_grade": stability_grade,
        "readiness_score": readiness.get("score"),
        "coverage_score": avg_cov,
        "feature_quality_score": feature_quality_score,
        "news_quality_score": news_quality_score,
    }


def _build_evidence_points(
    market_features: dict,
    capital_dims: dict,
    direction_forecast: dict,
    valuation: dict | None = None,
    market_state: str | None = None,
    news_count: int = 0,
    policy_count: int = 0,
    social_count: int = 0,
    recommendation: str = "HOLD",
) -> list[dict]:
    """Build dynamic evidence cards based on available data.

    Returns a list of dicts with keys: title, badge, badge_type, basis, nums.
    badge_type: 'up'|'down'|'flat'|'warn'
    """
    points: list[dict] = []
    features = (market_features or {}).get("features") or {}

    # 1. 技术趋势（始终可用）
    trend = features.get("trend") or "震荡"
    ma5 = features.get("ma5")
    ma20 = features.get("ma20")
    ret5 = features.get("ret5")
    macd_info = features.get("macd") or {}
    kdj_info = features.get("kdj") or {}
    boll_info = features.get("boll") or {}
    dif = macd_info.get("dif")
    dea = macd_info.get("dea")
    j_val = kdj_info.get("j")

    tech_badge = "up" if "偏多" in trend or "上涨" in trend else "down" if "偏空" in trend or "下跌" in trend else "flat"
    tech_nums = [f"MA5={ma5 or '—'}", f"MA20={ma20 or '—'}"]
    if ret5 is not None:
        tech_nums.append(f"近5日收益={ret5}%")
    if isinstance(dif, (int, float)) and isinstance(dea, (int, float)):
        tech_nums.append(f"MACD {'金叉' if dif > dea else '死叉'}")
    if isinstance(j_val, (int, float)):
        tech_nums.append(f"KDJ-J={round(j_val, 1)}{'(超买)' if j_val > 80 else '(超卖)' if j_val < 20 else ''}")

    tech_basis = f"均线显示 {trend}。"
    if ma5 and ma20:
        rel = "MA5在MA20上方" if ma5 > ma20 else "MA5在MA20下方"
        tech_basis = f"{rel}，近5日收益{ret5}%，趋势{trend}。"

    points.append({
        "title": "技术趋势",
        "badge": trend,
        "badge_type": tech_badge,
        "basis": tech_basis,
        "nums": tech_nums,
    })

    # 2. 资金面（只要有任意一项数据就显示）
    capital_flow = (capital_dims or {}).get("capital_flow") or {}
    dragon_tiger = (capital_dims or {}).get("dragon_tiger") or {}
    margin = (capital_dims or {}).get("margin_financing") or {}
    mf = capital_flow.get("main_force") or {}
    mf5 = mf.get("net_inflow_5d")
    lhb30 = dragon_tiger.get("lhb_count_30d")
    rz5 = margin.get("rzye_delta_5d")

    cap_nums = []
    cap_parts = []
    has_cap_data = False

    if mf5 is not None:
        has_cap_data = True
        mf5_str = _fmt_yuan(mf5)
        cap_nums.append(f"主力5日净流={mf5_str}")
        cap_parts.append(f"主力净流{mf5_str}")
    if lhb30 is not None:
        has_cap_data = True
        cap_nums.append(f"龙虎榜30日={lhb30}次")
        cap_parts.append(f"龙虎榜{lhb30}次")
    if rz5 is not None:
        has_cap_data = True
        rz5_str = _fmt_yuan(rz5)
        cap_nums.append(f"两融5日变化={rz5_str}")
        cap_parts.append(f"两融变化{rz5_str}")
    nb = capital_dims.get("capital_flow") or {}
    nb_obj = nb.get("northbound") or {}
    if nb_obj.get("status") == "ok" and nb_obj.get("net_inflow_5d") is not None:
        nb5_str = _fmt_yuan(nb_obj.get("net_inflow_5d"))
        cap_nums.append(f"北向5日净流={nb5_str}")
        cap_parts.append(f"北向5日{nb5_str}")
    else:
        cap_nums.append("北向逐股数据暂不支持")

    if has_cap_data:
        cap_badge_type = "up" if (mf5 or 0) > 0 else "down" if (mf5 or 0) < 0 else "flat"
        cap_badge = "净流入" if (mf5 or 0) > 0 else "净流出" if (mf5 or 0) < 0 else "中性"
        basis_text = "；".join(cap_parts) + "。" if cap_parts else ("主力资金数据可用。" if nb_obj.get("status") == "ok" else "主力资金数据可用，北向逐股数据暂不支持。")
        points.append({
            "title": "资金面",
            "badge": cap_badge,
            "badge_type": cap_badge_type,
            "basis": basis_text,
            "nums": cap_nums,
        })

    # 3. 消息面（新闻+政策+热搜，有任意内容则显示）
    total_info = (news_count or 0) + (policy_count or 0) + (social_count or 0)

    if total_info > 0:
        info_parts = []
        if news_count > 0:
            info_parts.append(f"新闻{news_count}条")
        if policy_count > 0:
            info_parts.append(f"政策{policy_count}条")
        if social_count > 0:
            info_parts.append(f"热搜{social_count}条")
        info_badge_type = "warn" if social_count > 0 else "flat"
        info_badge = "有关注" if social_count > 0 else "正常"
        points.append({
            "title": "消息面",
            "badge": info_badge,
            "badge_type": info_badge_type,
            "basis": f"收录{'、'.join(info_parts)}，已进行情绪加权分析。",
            "nums": [f"新闻={news_count}条", f"政策={policy_count}条", f"热搜={social_count}条"],
        })

    # 4. 预测可靠性（优先看 7 日可操作统计）
    d7 = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 7), None) or {}
    d7b = next((x for x in (direction_forecast.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), None) or {}
    d7_acc = d7b.get("actionable_accuracy")
    d7_samples = d7b.get("actionable_samples")
    d7_cov = d7b.get("actionable_coverage")
    reliable = bool(
        isinstance(d7_samples, int)
        and d7_samples >= 5
        and isinstance(d7_cov, (int, float))
        and d7_cov >= 0.05
    )
    points.append(
        {
            "title": "预测可靠性",
            "badge": "可执行" if reliable else "偏弱",
            "badge_type": "up" if reliable else "warn",
            "basis": "7天可操作统计用于过滤看起来很强但不可执行的信号。",
            "nums": [
                f"7天方向={d7.get('direction') or '—'}",
                f"7天准确率={d7_acc if d7_acc is not None else '—'}",
                f"样本={d7_samples if d7_samples is not None else '—'}",
                f"覆盖率={d7_cov if d7_cov is not None else '—'}",
            ],
        }
    )

    # 5. 基本面（估值）
    v = valuation or {}
    if any(v.get(k) is not None for k in ("pe_ttm", "pb", "total_market_cap", "market_cap", "industry")):
        points.append(
            {
                "title": "基本面",
                "badge": "估值快照",
                "badge_type": "flat",
                "basis": f"行业={v.get('industry') or '—'}，用于避免纯技术面单维决策。",
                "nums": [
                    f"PE={v.get('pe_ttm') if v.get('pe_ttm') is not None else '—'}",
                    f"PB={v.get('pb') if v.get('pb') is not None else '—'}",
                    f"总市值={v.get('total_market_cap') if v.get('total_market_cap') is not None else v.get('market_cap') if v.get('market_cap') is not None else '—'}",
                ],
            }
        )

    # 6. 市场状态（环境闸）
    if market_state:
        points.append(
            {
                "title": "市场状态",
                "badge": market_state,
                "badge_type": "up" if str(market_state).upper() == "BULL" else "down" if str(market_state).upper() == "BEAR" else "flat",
                "basis": "先判断大盘环境，再决定个股仓位上限。",
                "nums": [f"market_state={market_state}"],
            }
        )

    # 7. 综合判断（始终作为压轴）
    rec_map = {"BUY": ("建议介入", "up"), "SELL": ("建议回避", "down"), "HOLD": ("观望等待", "flat")}
    rec_label, rec_badge_type = rec_map.get(recommendation, ("观望等待", "flat"))
    points.append({
        "title": "综合判断",
        "badge": rec_label,
        "badge_type": rec_badge_type,
        "basis": f"综合技术、资金与消息多维分析，最终结论：{rec_label}。",
        "nums": [f"建议={rec_label}"],
    })

    return points


def _build_plain_report(
    stock_code: str,
    recommendation: str,
    market_snapshot: dict,
    market_features: dict,
    direction_forecast: dict,
    price_forecast: dict,
    capital_dims: dict,
    quality_gate: dict,
    social_count: int,
    news_count: int,
    policy_count: int,
    llm_reason_text: str = "",
    llm_trigger: str = "",
    llm_invalidation: str = "",
    llm_risks: str = "",
    valuation: dict | None = None,
    market_state: str | None = None,
) -> dict:
    reco_cn = {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(recommendation, "观望等待")
    features = market_features.get("features", {})
    d1 = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 1), {})
    d7 = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 7), {})
    b7 = next((x for x in (direction_forecast.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), {})
    dual = _build_dual_market_brief(market_snapshot)
    chain = [
        {
            "step": 1,
            "title": "先看真实价格",
            "fact": f"当前价 {market_snapshot.get('last_price')}，今日涨跌 {market_snapshot.get('pct_change')}。",
            "impact": "这是所有判断的起点，价格本身决定你在高位还是低位附近。",
        },
        {
            "step": 2,
            "title": "再看趋势强弱",
            "fact": f"MA5={features.get('ma5')}，MA20={features.get('ma20')}，趋势={features.get('trend')}。",
            "impact": "短期均线在中期均线上方通常偏强，反之偏弱。",
        },
        {
            "step": 3,
            "title": "看未来1~7天方向",
            "fact": (
                f"1天方向={d1.get('direction','-')}，"
                f"7天方向={d7.get('direction','-')}，"
                f"7天动作={'减仓/卖出' if d7.get('action') == 'SELL' else '持有/加仓' if d7.get('action') == 'BUY' else '观望' if d7.get('action') == 'HOLD' else '-'}。"
            ),
            "impact": "方向信号用于给出操作动作（持有/卖出），不是只看一个点位。",
        },
        {
            "step": 4,
            "title": "做风险过滤",
            "fact": (
                f"7天回测准确率={'暂无' if b7.get('actionable_accuracy') is None else str(round(b7.get('actionable_accuracy')*100, 1))+'%'}，"
                f"样本={'暂无' if b7.get('actionable_samples') is None else b7.get('actionable_samples')}，"
                f"覆盖率={'暂无' if b7.get('actionable_coverage') is None else str(round(b7.get('actionable_coverage')*100, 1))+'%'}。"
            ),
            "impact": "如果样本太少或覆盖率太低，即使信号看起来好，也要降低仓位或先观望。",
        },
        {
            "step": 5,
            "title": "结合外部事件",
            "fact": f"新闻={news_count}条，政策={policy_count}条，相关热搜={social_count}条。",
            "impact": "事件信息用于验证趋势是否容易被突发消息打断。",
        },
    ]
    terms = [
        {"term": "MA5", "plain_explain": "最近5个交易日平均价格，反映短期方向。"},
        {"term": "MA20", "plain_explain": "最近20个交易日平均价格，反映中期方向。"},
        {"term": "覆盖率", "plain_explain": "模型在多少比例的样本里给出明确动作。"},
        {"term": "样本数", "plain_explain": "回测里可用于统计的次数，越多通常越可靠。"},
    ]
    d7_cov = b7.get("actionable_coverage") if isinstance(b7.get("actionable_coverage"), (int, float)) else 0.0
    d7_samples = b7.get("actionable_samples") if isinstance(b7.get("actionable_samples"), int) else 0
    ret5 = features.get("ret5")
    stop_loss_hint = None
    if isinstance(ret5, (int, float)):
        stop_loss_hint = round(float(market_snapshot.get("last_price") or 0) * (1 - min(0.06, max(0.02, abs(ret5) * 1.8))), 3)
    if recommendation == "SELL":
        position_text = "建议仓位：0%~20%（以防守为主，优先减仓）"
        action_text = "若已持仓，优先分批减仓；新资金暂不入场。"
    elif recommendation == "BUY":
        position_text = "建议仓位：40%~70%（分批入场，不追高）"
        action_text = "首次仓位不超过计划仓位的一半，确认趋势后再加仓。"
    else:
        position_text = "建议仓位：20%~40%（轻仓观察）"
        action_text = "先观察信号稳定性，避免在消息波动日重仓交易。"
    reliability_tag = "可执行性较好" if (d7_cov >= 0.05 and d7_samples >= 5) else "可执行性偏弱（样本不足）"
    capital_flow = (capital_dims or {}).get("capital_flow") or {}
    dragon_tiger = (capital_dims or {}).get("dragon_tiger") or {}
    margin = (capital_dims or {}).get("margin_financing") or {}
    mf5 = ((capital_flow.get("main_force") or {}).get("net_inflow_5d"))
    north5 = ((capital_flow.get("northbound") or {}).get("net_inflow_5d"))
    lhb30 = dragon_tiger.get("lhb_count_30d")
    rz5 = margin.get("rzye_delta_5d")
    
    # Sparklines — capital_flow.py exposes recent 10-day net inflows as "recent_net_inflows" (list of floats).
    mf_hist = (capital_flow.get("main_force") or {}).get("recent_net_inflows") or []
    
    mf_obj = capital_flow.get("main_force") or {}
    nb_obj = dict(capital_flow.get("northbound") or {})
    nb5_val = nb_obj.get("net_inflow_5d")
    nb_obj["net_inflow_5d_fmt"] = _fmt_yuan(nb5_val) if nb5_val is not None else None
    capital_game_summary = {
        "headline": "资金博弈：主力资金、龙虎榜、两融、北向与ETF维度综合观察。",
        "northbound": nb_obj,
        "main_force": {
            **mf_obj,
            "net_inflow_5d_fmt": _fmt_yuan(mf_obj.get("net_inflow_5d")),
            "net_inflow_1d_fmt": _fmt_yuan(mf_obj.get("net_inflow_1d")),
            "net_inflow_10d_fmt": _fmt_yuan(mf_obj.get("net_inflow_10d")),
        },
        "dragon_tiger": {
            "source": dragon_tiger.get("source"),
            "lhb_count_30d": lhb30,
            "lhb_count_90d": dragon_tiger.get("lhb_count_90d"),
            "net_buy_total": dragon_tiger.get("net_buy_total"),
            "avg_net_buy_ratio": dragon_tiger.get("avg_net_buy_ratio"),
            "seat_concentration": dragon_tiger.get("seat_concentration"),
        },
        "margin_financing": {
            "status": margin.get("status"),
            "reason": margin.get("reason"),
            "latest_rzye": margin.get("latest_rzye"),
            "latest_rqye": margin.get("latest_rqye"),
            "latest_rzye_fmt": _fmt_yuan(margin.get("latest_rzye")),
            "rzye_delta_5d": rz5,
            "rzye_delta_5d_fmt": _fmt_yuan(rz5),
            "rzye_delta_20d": margin.get("rzye_delta_20d"),
            "rzye_delta_20d_fmt": _fmt_yuan(margin.get("rzye_delta_20d")),
            "rqye_delta_5d": margin.get("rqye_delta_5d"),
            "rqye_delta_20d": margin.get("rqye_delta_20d"),
        },
        "summary_text": (
            f"近5日主力净流={'暂无' if mf5 is None else _fmt_yuan(mf5)}，"
            f"北向5日净流={'暂无' if north5 is None else _fmt_yuan(north5)}，"
            f"龙虎榜近30日上榜={lhb30 if lhb30 is not None else '—'}次，两融融资余额5日变化={'暂无' if rz5 is None else _fmt_yuan(rz5)}。"
        ),
        "history_span": {
            "main_force_records": (capital_flow.get("main_force") or {}).get("history_records"),
            "main_force_range": [
                (capital_flow.get("main_force") or {}).get("history_start_date"),
                (capital_flow.get("main_force") or {}).get("history_end_date"),
            ],
            "dragon_tiger_records": dragon_tiger.get("history_records"),
            "dragon_tiger_range": [dragon_tiger.get("history_start_date"), dragon_tiger.get("history_end_date")],
            "dragon_tiger_page_stats": dragon_tiger.get("page_stats") or {},
            "margin_records": margin.get("history_records"),
            "margin_range": [margin.get("history_start_date"), margin.get("history_end_date")],
            "margin_page_stats": margin.get("page_stats") or {},
        },

    }
    
    # Add Technical Analysis Structured Data
    f = market_features.get("features", {})
    technical_analysis = {
         "ma5": f.get("ma5"),
         "ma20": f.get("ma20"),
         "trend": f.get("trend"),
         "macd_status": "金叉" if (f.get("macd", {}).get("dif", 0) or 0) > (f.get("macd", {}).get("dea", 0) or 0) else "死叉", 
         "kdj_status": "超买" if (f.get("kdj", {}).get("j", 50) or 50) > 80 else ("超卖" if (f.get("kdj", {}).get("j", 50) or 50) < 20 else "常态"),
         "boll_status": "中轨上方" if (market_snapshot.get("last_price") or 0) > (f.get("boll", {}).get("mid") or 0) else "中轨下方",
    }
    
    # Add Sparklines
    mf_recent = ((capital_flow.get("main_force") or {}).get("recent_net_inflows") or [])
    capital_game_summary["main_force"]["sparkline"] = _generate_unicode_sparkline(mf_recent)

    # Build dynamic evidence points based on available data
    evidence_backing_points = _build_evidence_points(
        market_features=market_features,
        capital_dims=capital_dims,
        direction_forecast=direction_forecast,
        valuation=valuation,
        market_state=market_state,
        news_count=news_count,
        policy_count=policy_count,
        social_count=social_count,
        recommendation=recommendation,
    )
    stability_gate_result = {
        "publish_decision": quality_gate.get("publish_decision"),
        "stability_grade": quality_gate.get("stability_grade"),
        "coverage_score": quality_gate.get("coverage_score"),
        "degrade_reason": quality_gate.get("degrade_reason"),
    }
    execution_plan = {
        "position_suggestion": position_text,
        "risk_line": (
            f"风险线：价格跌破 {stop_loss_hint} 附近时，优先执行防守动作。"
            if stop_loss_hint is not None
            else "风险线：若连续2天走弱并跌破关键均线，优先防守。"
        ),
        "next_checklist": [
            f"检查7天信号：方向={d7.get('direction')}，动作={d7.get('action')}。",
            f"检查信号稳定性：样本={d7_samples}，覆盖率={round(d7_cov,4)}，{reliability_tag}。",
            f"检查外部事件：新闻{news_count}条、政策{policy_count}条、热搜{social_count}条是否出现突发变化。",
        ],
        "execution_note": action_text,
    }
    return {
        "title": f"{stock_code} 白话研报",
        "action_now": reco_cn,
        "one_sentence": f"结论：{reco_cn}。先看真实双源行情，再看1~7天方向和回测稳定性。",
        "reason": llm_reason_text,
        "trigger": llm_trigger,
        "invalidation": llm_invalidation,
        "risks": llm_risks,
        "execution_plan": execution_plan,
        "what_to_do_now": [
            f"当前建议：{reco_cn}。",
            "先确认7天信号是否稳定（样本和覆盖率都不能太低）。",
            "出现重大公告或政策变化时，优先重新生成报告。",
        ],
        "key_numbers": [
            {"name": "当前价格", "value": market_snapshot.get("last_price"), "why": "决定你当前买卖位置。"},
            {"name": "7天方向", "value": d7.get("direction"), "why": "用于判断一周内主方向。"},
            {"name": "7天回测准确率", "value": b7.get("actionable_accuracy"), "why": "衡量信号近期是否靠谱。"},
        ],
        "accuracy_explain": _build_accuracy_explain(direction_forecast, price_forecast),
        "capital_game_summary": capital_game_summary,
        "evidence_backing_points": evidence_backing_points,
        "stability_gate_result": stability_gate_result,
        "cause_effect_chain": chain,
        "dual_source_market": dual,
        "terms": terms,
    }


def _timeliness_hours(v) -> float:
    if isinstance(v, datetime):
        dt = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    elif isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    return round(max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0), 3)


def _jsonable(v):
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    return v


def _ensure_enhanced_schema(report: dict) -> tuple[dict, bool]:
    data = dict(report or {})
    changed = False
    dims = dict(data.get("dimensions") or {})
    if "capital_flow" not in dims:
        dims["capital_flow"] = {
            "stock_code": data.get("stock_code"),
            "northbound": {"status": "missing"},
            "main_force": {"status": "missing", "history_records": 0},
            "etf_flow": {"status": "missing"},
        }
        changed = True
    else:
        cap = dict(dims.get("capital_flow") or {})
        mf = dict(cap.get("main_force") or {})
        if "history_records" not in mf:
            mf["history_records"] = 0
            changed = True
        cap["main_force"] = mf
        dims["capital_flow"] = cap
    if "dragon_tiger" not in dims:
        dims["dragon_tiger"] = {
            "status": "missing",
            "reason": "cached_report_missing_field",
            "history_records": 0,
            "page_stats": {
                "total_pages_reported": None,
                "pages_fetched": 0,
                "records_fetched": 0,
                "truncated_by_safety_limit": False,
                "truncated_by_limit": False,
                "records_after_limit": 0,
            },
            "lhb_count_30d": 0,
            "lhb_count_90d": 0,
            "lhb_count_250d": 0,
            "latest_records": [],
        }
        changed = True
    else:
        dg = dict(dims.get("dragon_tiger") or {})
        if "history_records" not in dg:
            dg["history_records"] = 0
            changed = True
        if "latest_records" not in dg:
            dg["latest_records"] = []
            changed = True
        if "page_stats" not in dg:
            dg["page_stats"] = {
                "total_pages_reported": None,
                "pages_fetched": 0,
                "records_fetched": 0,
                "truncated_by_safety_limit": False,
                "truncated_by_limit": False,
                "records_after_limit": 0,
            }
            changed = True
        for k in ("lhb_count_30d", "lhb_count_90d", "lhb_count_250d"):
            if k not in dg:
                dg[k] = 0
                changed = True
        dims["dragon_tiger"] = dg
    if "margin_financing" not in dims:
        dims["margin_financing"] = {
            "status": "missing",
            "reason": "cached_report_missing_field",
            "history_records": 0,
            "page_stats": {
                "total_pages_reported": None,
                "pages_fetched": 0,
                "records_fetched": 0,
                "truncated_by_safety_limit": False,
                "truncated_by_limit": False,
                "records_after_limit": 0,
            },
            "margin_balance_series": [],
        }
        changed = True
    else:
        mg = dict(dims.get("margin_financing") or {})
        if "history_records" not in mg:
            mg["history_records"] = 0
            changed = True
        if "margin_balance_series" not in mg:
            mg["margin_balance_series"] = []
            changed = True
        if "page_stats" not in mg:
            mg["page_stats"] = {
                "total_pages_reported": None,
                "pages_fetched": 0,
                "records_fetched": 0,
                "truncated_by_safety_limit": False,
                "truncated_by_limit": False,
                "records_after_limit": 0,
            }
            changed = True
        dims["margin_financing"] = mg
    data["dimensions"] = dims

    pf = dict(data.get("price_forecast") or {})
    ex = dict(pf.get("explain") or {})
    hp = dict(ex.get("historical_position") or {})
    if "historical_position_long" not in ex:
        ex["historical_position_long"] = {
            "percentile_3y": hp.get("percentile_3y"),
            "percentile_5y": hp.get("percentile_5y"),
            "percentile_all": hp.get("percentile_all"),
            "extreme_3y": hp.get("extreme_3y"),
            "extreme_5y": hp.get("extreme_5y"),
            "extreme_all": hp.get("extreme_all"),
        }
        changed = True
    if "multi_cycle_alignment" not in ex:
        ex["multi_cycle_alignment"] = {"regime_tag": "震荡", "agree_all": None}
        changed = True
    if "regime_tag" not in ex:
        ex["regime_tag"] = (ex.get("multi_cycle_alignment") or {}).get("regime_tag")
        changed = True
    pf["explain"] = ex
    data["price_forecast"] = pf

    if "quality_gate" not in data:
        data["quality_gate"] = {
            "publish_decision": "degraded",
            "degrade_reason": ["cached_report_missing_field"],
            "dimension_coverage": {"market_snapshot": 1.0},
            "stability_grade": "C",
            "coverage_score": 0.0,
            "missing_fields": [],
            "recover_actions": [],
        }
        changed = True

    plain = dict(data.get("plain_report") or {})
    if "capital_game_summary" not in plain:
        plain["capital_game_summary"] = {
            "headline": "资金维度待补齐",
            "summary_text": "旧缓存报告暂无资金博弈明细。",
            "history_span": {
                "main_force_records": None,
                "main_force_range": [None, None],
                "dragon_tiger_records": None,
                "dragon_tiger_range": [None, None],
                "dragon_tiger_page_stats": {},
                "margin_records": None,
                "margin_range": [None, None],
                "margin_page_stats": {},
            },
        }
        changed = True

    else:
        cgs = dict(plain.get("capital_game_summary") or {})
        if "history_span" not in cgs:
            cgs["history_span"] = {
                "main_force_records": None,
                "main_force_range": [None, None],
                "dragon_tiger_records": None,
                "dragon_tiger_range": [None, None],
                "dragon_tiger_page_stats": {},
                "margin_records": None,
                "margin_range": [None, None],
                "margin_page_stats": {},
            }
            plain["capital_game_summary"] = cgs
            changed = True
            
    # Add Technical Analysis Structured Data
    dims_for_plain = dict(data.get("dimensions") or {})
    mf_for_plain = dict(dims_for_plain.get("market_features") or {})
    features_for_plain = dict(mf_for_plain.get("features") or {})
    if "technical_analysis" not in plain:
        plain["technical_analysis"] = {
            "ma5": features_for_plain.get("ma5"),
            "ma20": features_for_plain.get("ma20"),
            "trend": features_for_plain.get("trend"),
            "macd_status": "未知 - 需TDX数据",  # Placeholder if missing
            "kdj_status": "未知 - 需TDX数据",
            "boll_status": "未知 - 需TDX数据",
        }
        changed = True
    else:
        ta = dict(plain.get("technical_analysis") or {})
        if "ma5" not in ta:
            ta["ma5"] = features_for_plain.get("ma5")
            changed = True
        if "ma20" not in ta:
            ta["ma20"] = features_for_plain.get("ma20")
            changed = True
        if "trend" not in ta:
            ta["trend"] = features_for_plain.get("trend")
            changed = True
        ta.setdefault("macd_status", "未知 - 需TDX数据")
        ta.setdefault("kdj_status", "未知 - 需TDX数据")
        ta.setdefault("boll_status", "未知 - 需TDX数据")
        plain["technical_analysis"] = ta
        
    if "evidence_backing_points" not in plain:
        plain["evidence_backing_points"] = []
        changed = True
    if "stability_gate_result" not in plain:
        qg = data.get("quality_gate") or {}
        plain["stability_gate_result"] = {
            "publish_decision": qg.get("publish_decision"),
            "stability_grade": qg.get("stability_grade"),
            "coverage_score": qg.get("coverage_score"),
            "degrade_reason": qg.get("degrade_reason"),
        }
        changed = True
    data["plain_report"] = plain
    bt = ((data.get("price_forecast") or {}).get("backtest") or {})
    sm = dict(bt.get("summary") or {})
    if "opportunities" not in sm:
        sm["opportunities"] = sm.get("samples", 0)
        changed = True
    if "warmup_days" not in sm:
        sm["warmup_days"] = 80
        changed = True
    sm3 = dict(bt.get("summary_recent_3m") or {})
    if "opportunities" not in sm3:
        sm3["opportunities"] = sm3.get("samples", 0)
        changed = True
    if "method" not in sm or "择优模型" not in str(sm.get("method") or ""):
        sm["method"] = "滚动动量回测（按窗口择优模型）"
        changed = True
    bt["summary"] = sm
    if sm3:
        bt["summary_recent_3m"] = sm3
    pf = dict(data.get("price_forecast") or {})
    pf["backtest"] = bt
    data["price_forecast"] = pf
    return data, changed


def _fallback_market_features_from_tdx(market_features: dict, tdx_local: dict) -> dict:
    # 1. Base fallback logic
    out = market_features
    use_fallback = True
    if market_features.get("status") == "ok" and (market_features.get("features") or {}).get("ma5") is not None:
        use_fallback = False
    
    if use_fallback:
        series = (tdx_local or {}).get("series") or []
        if len(series) >= 25:
            closes = [float(x["close"]) for x in series]
            ma5 = round(sum(closes[-5:]) / 5.0, 4)
            ma20 = round(sum(closes[-20:]) / 20.0, 4)
            ret5 = round((closes[-1] - closes[-6]) / closes[-6], 6) if closes[-6] else None
            ret20 = round((closes[-1] - closes[-21]) / closes[-21], 6) if closes[-21] else None
            trend = "偏多" if ma5 > ma20 else "偏空"
            out = {
                "source": "tdx_local_fallback",
                "status": "ok",
                "features": {
                    "ma5": ma5,
                    "ma20": ma20,
                    "ret5": ret5,
                    "ret20": ret20,
                    "trend": trend,
                    "sample_days": len(series),
                    "last_trade_date": series[-1].get("date"),
                },
            }

    # 2. Enrich with advanced indicators from TDX
    dest_features = out.get("features") or {}
    src_features = (tdx_local or {}).get("features") or {}
    for k in ["macd", "kdj", "rsi6", "rsi12", "rsi14", "rsi24", "boll", "atr14"]:
        if k in src_features:
            dest_features[k] = src_features[k]
    
    out["features"] = dest_features
    return out


def _industry_keywords(stock_code: str) -> list[str]:
    import json
    try:
        raw = getattr(settings, "stock_industry_keywords", "") or "{}"
        mapping = json.loads(raw) if isinstance(raw, str) else {}
    except Exception:
        mapping = {"600519": ["白酒", "消费", "酱香", "高端白酒"], "000001": ["银行", "信贷", "金融"], "300750": ["锂电", "动力电池", "新能源车"]}
    return mapping.get(stock_code.split(".")[0], [])


async def _collect_historical_social(
    db: Session,
    stock_code: str,
    stock_name: str | None,
    existing_titles: set[str],
    needed: int,
) -> list[dict]:
    if needed <= 0:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.hotspot_max_age_hours)
    rows = (
        db.query(HotspotRaw)
        .filter(HotspotRaw.fetch_time >= cutoff)
        .order_by(HotspotRaw.fetch_time.desc())
        .limit(200)
        .all()
    )
    result: list[dict] = []
    industry = _industry_keywords(stock_code)
    for row in rows:
        title = (row.title or "").strip()
        if not title or title in existing_titles:
            continue
        source_url = str(row.source_url or "")
        if not source_url.startswith("http"):
            continue
        link = link_topic_to_stock(title, stock_code, stock_name=stock_name, industry_keywords=industry)
        if link["relevance_score"] < settings.hotspot_relevance_threshold:
            continue
        hours = _timeliness_hours(row.fetch_time)
        if hours > settings.hotspot_max_age_hours:
            continue

        result.append(
            {
                "topic_id": topic_id_for_title(title),
                "platform": row.platform,
                "rank": row.rank,
                "title": title,
                "raw_heat": row.raw_heat,
                "fetch_time": row.fetch_time,
                "source_url": source_url,
                "heat_score": round(max(0.1, 1.0 - (row.rank / 100.0)), 4),
                "relevance_score": link["relevance_score"],
                "match_method": f"historical+{link['match_method']}",
                "timeliness_hours": hours,
                "decay_weight": compute_decay_weight(row.fetch_time),
                "sentiment_score": 0.0,
                "event_type": "general",
            }
        )
        existing_titles.add(title)
        if len(result) >= needed:
            break
    return result


async def _collect_realtime_social(db: Session, stock_code: str, stock_name: str | None = None) -> list[dict]:
    import asyncio as _asyncio
    weibo_results, douyin_results = await _asyncio.gather(
        fetch_weibo_hot(settings.hotspot_realtime_top_n),
        fetch_douyin_hot(settings.hotspot_realtime_top_n),
        return_exceptions=True,
    )
    topics = (weibo_results if isinstance(weibo_results, list) else [])
    topics += (douyin_results if isinstance(douyin_results, list) else [])

    dedup: dict[str, dict] = {}
    for t in topics:
        title = (t.get("title") or "").strip()
        source_url = str(t.get("source_url", ""))
        if not title or not source_url.startswith("http"):
            continue
        if title not in dedup:
            dedup[title] = t

    related: list[dict] = []
    industry = _industry_keywords(stock_code)
    for topic in dedup.values():
        link = link_topic_to_stock(
            topic["title"],
            stock_code,
            stock_name=stock_name,
            industry_keywords=industry,
        )
        if link["relevance_score"] < settings.hotspot_relevance_threshold:
            continue
        hours = _timeliness_hours(topic.get("fetch_time"))
        if hours > settings.hotspot_max_age_hours:
            continue

        norm = enrich_topic(topic)
        db.add(
            HotspotRaw(
                platform=topic["platform"],
                rank=topic["rank"],
                title=topic["title"],
                raw_heat=topic["raw_heat"],
                fetch_time=topic["fetch_time"],
                source_url=topic["source_url"],
                cookie_version="v1",
            )
        )
        db.merge(HotspotNormalized(**norm))
        db.add(
            HotspotStockLink(
                topic_id=topic["topic_id"],
                stock_code=stock_code,
                relevance_score=link["relevance_score"],
                match_method=link["match_method"],
            )
        )
        related.append(
            {
                "topic_id": topic["topic_id"],
                "platform": topic["platform"],
                "rank": topic["rank"],
                "title": topic["title"],
                "raw_heat": topic["raw_heat"],
                "fetch_time": topic["fetch_time"],
                "source_url": topic["source_url"],
                "heat_score": topic.get("heat_score", 0.0),
                "relevance_score": link["relevance_score"],
                "match_method": link["match_method"],
                "timeliness_hours": hours,
                "decay_weight": norm["decay_weight"],
                "sentiment_score": norm["sentiment_score"],
                "event_type": norm["event_type"],
            }
        )

    db.commit()

    existing_titles = {x["title"] for x in related}
    if len(related) < settings.hotspot_min_related_topics:
        needed = settings.hotspot_min_related_topics - len(related)
        related.extend(
            await _collect_historical_social(
                db,
                stock_code=stock_code,
                stock_name=stock_name,
                existing_titles=existing_titles,
                needed=needed,
            )
        )

    related.sort(
        key=lambda x: (
            x.get("relevance_score", 0.0) * 0.5
            + x.get("decay_weight", 0.0) * 0.3
            + x.get("heat_score", 0.0) * 0.2
        ),
        reverse=True,
    )
    return related[:10]


def _build_reasoning_evidence(citations: list[dict]) -> list[dict]:
    items = []
    for idx, c in enumerate(citations, start=1):
        items.append(
            {
                "evidence_id": f"E{idx:02d}",
                "source_name": c.get("source_name"),
                "title": c.get("title"),
                "source_url": c.get("source_url"),
                "fetch_time": c.get("fetch_time"),
                "relevance_score": c.get("relevance_score"),
                "timeliness_hours": c.get("timeliness_hours"),
            }
        )
    return items


def _reuse_recent_items(db: Session, stock_code: str, field_name: str, limit: int, max_age_hours: int = 168) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    rows = (
        db.query(Report)
        .filter(Report.stock_code == stock_code, Report.created_at >= cutoff)
        .order_by(Report.created_at.desc())
        .limit(30)
        .all()
    )
    out: list[dict] = []
    seen = set()
    for row in rows:
        raw = ((row.content_json or {}).get("reasoning_trace") or {}).get("raw_inputs") or {}
        arr = raw.get(field_name) or []
        for item in arr:
            url = str(item.get("source_url") or "").strip()
            title = str(item.get("title") or "").strip()
            if not url.startswith("http") or not title:
                continue
            if url in seen:
                continue
            seen.add(url)
            out.append(
                {
                    "source_name": item.get("source_name") or "历史证据回退",
                    "source_url": url,
                    "title": title,
                    "fetch_time": item.get("fetch_time") or datetime.now(timezone.utc).isoformat(),
                    "category": item.get("category") or ("policy" if "policy" in field_name else "news"),
                }
            )
            if len(out) >= limit:
                return out
    return out


def _reuse_recent_object(
    db: Session,
    stock_code: str,
    field_name: str,
    max_age_hours: int = 168,
    required_keys: list[str] | None = None,
) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    rows = (
        db.query(Report)
        .filter(Report.stock_code == stock_code, Report.created_at >= cutoff)
        .order_by(Report.created_at.desc())
        .limit(30)
        .all()
    )
    for row in rows:
        payload = row.content_json or {}
        obj = payload.get(field_name) or {}
        if _has_any_required_value(obj, required_keys=required_keys):
            return obj
        raw = ((payload.get("reasoning_trace") or {}).get("raw_inputs") or {}).get(field_name) or {}
        if _has_any_required_value(raw, required_keys=required_keys):
            return raw
    return {}


async def generate_report(
    db: Session,
    stock_code: str,
    run_mode: str,
    trade_date: str | None = None,
    idempotency_key: str | None = None,
    source: str = "real",
) -> tuple[Report, bool]:
    effective_trade_date = (trade_date or "").strip() or trade_date_str()
    if run_mode in ("daily", "tier2") and not idempotency_key:
        idempotency_key = f"{run_mode}:{stock_code}:{effective_trade_date}"

    if idempotency_key:
        idem = db.get(ReportIdempotency, idempotency_key)
        if idem:
            existed = db.get(Report, idem.report_id)
            if existed:
                patched, changed = _ensure_enhanced_schema(existed.content_json or {})
                if changed:
                    existed.content_json = patched
                    db.merge(existed)
                    db.commit()
                should_rebuild = (run_mode == "daily") and _should_rebuild_daily_idempotent_report(
                    existed.content_json or {},
                    existed.created_at,
                )
                if not should_rebuild:
                    return (existed, True)

    market_snapshot, market_features, company_overview, valuation, capital_dims = await gather(
        fetch_quote_snapshot(stock_code),
        fetch_market_features(stock_code),
        fetch_company_overview(stock_code),
        fetch_valuation_snapshot(stock_code),
        fetch_capital_dimensions(stock_code),
    )
    tdx_local = build_tdx_local_features(stock_code)
    # When TDX local data is missing, backfill series from capital_flow kline cache
    # so that direction_forecast and backtest can use K-line close data.
    if not (tdx_local.get("series") or []):
        from app.services.capital_flow import _load_cache as _cf_load_cache
        _cf_cache = _cf_load_cache(stock_code)
        _cf_rows = sorted(
            [r for r in (_cf_cache.get("capital_flow_rows") or []) if r.get("close") is not None],
            key=lambda x: str(x.get("date") or "")
        )
        if len(_cf_rows) >= 30:
            # Build minimal tdx_local-compatible series from kline cache
            tdx_local = {
                "status": "kline_proxy",
                "features": tdx_local.get("features") or {},
                "series": [{"date": r.get("date"), "close": r.get("close")} for r in _cf_rows],
            }
    market_features = _fallback_market_features_from_tdx(market_features, tdx_local)
    market_breadth = load_market_breadth_latest()
    degraded_mode = market_snapshot.get("source") == "fallback" or market_snapshot.get("last_price") is None
    if settings.strict_real_data and degraded_mode:
        raise RuntimeError(f"real_market_data_unavailable: {market_snapshot.get('errors')}")

    if not company_overview.get("company_name") or company_overview.get("company_name") == stock_code:
        company_overview["company_name"] = market_snapshot.get("name") or stock_code
    if not company_overview.get("stock_code"):
        company_overview["stock_code"] = stock_code
    if not company_overview.get("industry") and valuation.get("industry"):
        company_overview["industry"] = valuation.get("industry")
    if not valuation.get("stock_name"):
        valuation["stock_name"] = market_snapshot.get("name")
    if valuation.get("total_market_cap") is None:
        valuation["total_market_cap"] = market_snapshot.get("amount")

    # Reuse recently validated snapshots when remote sources are unstable.
    if not company_overview.get("industry") or not company_overview.get("listed_date"):
        recent_overview = _reuse_recent_object(
            db,
            stock_code,
            "company_overview",
            max_age_hours=30 * 24,
            required_keys=["industry", "listed_date", "industry_csrc", "exchange", "market_type"],
        )
        for k in ("industry", "listed_date", "industry_csrc", "exchange", "market_type"):
            if not company_overview.get(k) and recent_overview.get(k):
                company_overview[k] = recent_overview.get(k)
    if valuation.get("pe_ttm") is None or valuation.get("pb") is None:
        recent_valuation = _reuse_recent_object(
            db,
            stock_code,
            "valuation",
            max_age_hours=30 * 24,
            required_keys=["pe_ttm", "pb", "industry"],
        )
        if valuation.get("pe_ttm") is None and recent_valuation.get("pe_ttm") is not None:
            valuation["pe_ttm"] = recent_valuation.get("pe_ttm")
        if valuation.get("pb") is None and recent_valuation.get("pb") is not None:
            valuation["pb"] = recent_valuation.get("pb")
        if not valuation.get("industry") and recent_valuation.get("industry"):
            valuation["industry"] = recent_valuation.get("industry")

    # Fallback to stock_master when remote sources and recent report cache are unavailable
    if not company_overview.get("industry") or valuation.get("total_market_cap") is None:
        from app.models import StockMaster as _StockMaster
        _sm = db.query(_StockMaster).filter(_StockMaster.stock_code == stock_code).first()
        if _sm:
            if not company_overview.get("industry") and _sm.industry:
                company_overview["industry"] = str(_sm.industry)
            if not company_overview.get("company_name") or company_overview.get("company_name") == stock_code:
                company_overview["company_name"] = str(_sm.stock_name or stock_code)
            if not company_overview.get("listed_date") and _sm.list_date:
                company_overview["listed_date"] = str(_sm.list_date)
            if not valuation.get("industry") and _sm.industry:
                valuation["industry"] = str(_sm.industry)
            if valuation.get("total_market_cap") is None:
                _price = market_snapshot.get("last_price") if isinstance(market_snapshot, dict) else None
                _shares = float(_sm.circulating_shares) if _sm.circulating_shares else None
                if _price and _shares:
                    try:
                        valuation["total_market_cap"] = round(float(_price) * _shares, 2)
                    except (TypeError, ValueError):
                        pass

    stock_name = company_overview.get("short_name") or market_snapshot.get("name") if isinstance(market_snapshot, dict) else None
    social_items = await _collect_realtime_social(db, stock_code, stock_name=stock_name)
    news_keywords = [company_overview.get("industry"), valuation.get("industry")] + _industry_keywords(stock_code)
    news_items, policy_items, industry_competition = await gather(
        fetch_stock_news(stock_code, stock_name=company_overview.get("company_name"), keywords=[x for x in news_keywords if x], limit=5),
        fetch_policy_news(5),
        fetch_industry_competition(stock_code, company_overview.get("industry") or valuation.get("industry")),
    )
    if len(news_items) == 0:
        news_items = _reuse_recent_items(db, stock_code, "news_items", limit=5, max_age_hours=7 * 24)
    if len(policy_items) == 0:
        policy_items = _reuse_recent_items(db, stock_code, "policy_items", limit=5, max_age_hours=7 * 24)
    if not industry_competition.get("industry_name"):
        industry_competition["industry_name"] = company_overview.get("industry") or valuation.get("industry")
    # Fallback valuation fields from industry component snapshot.
    if valuation.get("pe_ttm") is None or valuation.get("pb") is None:
        code = stock_code.split(".")[0]
        peers_raw = (industry_competition.get("raw") or {}).get("peers_raw") or []
        for p in peers_raw:
            if str(p.get("f12")) == code:
                if valuation.get("pe_ttm") is None:
                    valuation["pe_ttm"] = p.get("f9")
                if valuation.get("pb") is None:
                    valuation["pb"] = p.get("f23")
                break

    prompt = build_prompt(
        stock_code,
        market_snapshot,
        market_features,
        social_items,
        news_items,
        policy_items,
        tdx_local=tdx_local,
        capital_dims=capital_dims,
    )

    # 主报告 LLM 生成（建议/理由）— Tier-2 走 BULK_SCREEN 快速预判（13 §3.1）
    try:
        if run_mode == "tier2":
            from app.services.llm_router import LLMScene, route_and_call
            result = await route_and_call(prompt, scene=LLMScene.BULK_SCREEN, temperature=0.3, use_cot=False)
            llm = {"response": result.response, "model": result.model_used, "latency_ms": int(result.elapsed_s * 1000)}
        else:
            llm = await ollama_client.generate(prompt=prompt, use_prod=False)
        raw_text = (llm.get("response") or "").strip() or "观望"
    except Exception as _llm_err:
        import logging as _log
        _log.getLogger(__name__).error("主报告LLM调用失败，降级为HOLD: %s", _llm_err)
        raw_text = "观望"
    llm_reco = _recommendation_from_text(raw_text)
    rule_reco = _rule_recommendation(market_features)
    recommendation = llm_reco if llm_reco == rule_reco else "HOLD"
    llm_reason = _sanitize_llm_summary(raw_text)
    # Extract structured fields from LLM JSON output
    _llm_json_parsed = _parse_llm_forecast(raw_text)
    def _clean_text(t) -> str:
        """Remove duplicate punctuation and strip trailing whitespace.
        Accepts str or list (joins list with ；before cleaning)."""
        import re as _re
        if isinstance(t, list):
            t = "；".join(str(x) for x in t if x)
        t = _re.sub(r'。{2,}', '。', t or "")   # collapse repeated 。
        t = _re.sub(r'\.{2,}', '.', t)           # collapse repeated .
        return t.strip()

    llm_reason_text = _clean_text(_llm_json_parsed.get("reason") or llm_reason)
    llm_trigger = _clean_text(_llm_json_parsed.get("trigger") or "")
    llm_invalidation = _clean_text(_llm_json_parsed.get("invalidation") or "")
    llm_risks = _clean_text(_llm_json_parsed.get("risks") or "")

    # 多周期价格预测（LLM详细分析 + 技术公式辅助）
    price_forecast = await _calc_price_forecast(
        market_snapshot,
        market_features,
        social_items,
        tdx_local,
        market_breadth,
        capital_dims=capital_dims,
        policy_count=len(policy_items),
        stock_code=stock_code,
        news_items=news_items,
        policy_items=policy_items,
    )
    direction_forecast = price_forecast.get("direction_forecast") or {}

    # Recommendation fusion: use LLM 7-day forecast direction when available.
    # When multi-period LLM forecast overrides the main-report recommendation,
    # also update reason/trigger/invalidation/risks to be consistent with the
    # multi-period analysis (use 7d window reason as primary context).
    w7 = next((w for w in (price_forecast.get("windows") or []) if w.get("horizon_days") == 7), None)
    llm_w7_action = (w7 or {}).get("llm_action") or "HOLD"
    llm_w7_conf = float((w7 or {}).get("llm_confidence") or 0.0)
    d7_bt = next((x for x in (direction_forecast.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), None)
    dir_accuracy = float((d7_bt or {}).get("actionable_accuracy") or 0.0)
    llm_forecast_available = bool(price_forecast.get("llm_forecast_raw"))
    forecast_override_active = False
    # If LLM gave a directional forecast for 7d, use it as primary signal
    if llm_forecast_available and llm_w7_action in ("BUY", "SELL") and llm_w7_conf >= 0.5:
        recommendation = llm_w7_action
        forecast_override_active = True
    elif llm_reco == rule_reco:
        recommendation = llm_reco
    elif dir_accuracy >= 0.78:
        d7_fc = next((x for x in (direction_forecast.get("horizons") or []) if x.get("horizon_day") == 7), None)
        recommendation = (d7_fc or {}).get("action") or recommendation
    # else: keep HOLD

    # When multi-period forecast overrides main report, ensure reason is consistent.
    # Merge the multi-period 7d reason into the main reason if they conflict.
    w7_reason = (w7 or {}).get("llm_reason") or ""
    if forecast_override_active and w7_reason and llm_reason_text:
        # Prepend the 7d multi-period reasoning so users see why we say BUY/SELL
        if w7_reason not in llm_reason_text:
            main_text = llm_reason_text.rstrip("。").strip()
            w7_clean = w7_reason.rstrip("。").strip()
            merged = f"【7日预测依据】{w7_clean}。{main_text}"
            llm_reason_text = _clean_text(merged)

    recommendation, direction_override = _apply_direction_override(recommendation, direction_forecast)

    summary = _stable_summary_cn(
        recommendation,
        rule_reco,
        llm_reco,
        market_snapshot,
        market_features,
        len(social_items),
        len(news_items),
        len(policy_items),
        llm_reason_text or llm_reason,   # prefer parsed reason over raw sanitized text
    )
    # Use LLM-generated reason as the primary analysis summary; fall back to template.
    cn_summary = llm_reason_text if llm_reason_text else _cn_analysis(market_snapshot, market_features)
    novice_guide = _build_novice_guide(
        stock_code=stock_code,
        recommendation=recommendation,
        direction_forecast=direction_forecast,
        market_snapshot=market_snapshot,
        market_features=market_features,
        social_count=len(social_items),
        news_count=len(news_items),
        policy_count=len(policy_items),
    )
    quality_gate = _build_quality_gate(
        market_snapshot=market_snapshot,
        price_forecast=price_forecast,
        capital_dims=capital_dims,
        social_items=social_items,
        news_items=news_items,
        policy_items=policy_items,
    )

    plain_report = _build_plain_report(
        stock_code=stock_code,
        recommendation=recommendation,
        market_snapshot=market_snapshot,
        market_features=market_features,
        direction_forecast=direction_forecast,
        price_forecast=price_forecast,
        capital_dims=capital_dims,
        valuation=valuation,
        quality_gate=quality_gate,
        social_count=len(social_items),
        news_count=len(news_items),
        policy_count=len(policy_items),
        llm_reason_text=llm_reason_text,
        llm_trigger=llm_trigger,
        llm_invalidation=llm_invalidation,
        llm_risks=llm_risks,
    )

    report_id = uuid4().hex
    citations = [
        {
            "source_name": f"{market_snapshot.get('source')} 实时行情",
            "source_url": "https://quote.eastmoney.com" if market_snapshot.get("source") == "eastmoney" else "https://github.com/mootdx/mootdx",
            "fetch_time": market_snapshot.get("fetch_time"),
            "title": f"{stock_code} 最新价 {market_snapshot.get('last_price')}",
            "relevance_score": 1.0,
            "timeliness_hours": _timeliness_hours(market_snapshot.get("fetch_time")),
        },
        {
            "source_name": "公司资料",
            "source_url": f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code={'SH' + stock_code.split('.')[0] if stock_code.startswith('6') else 'SZ' + stock_code.split('.')[0]}",
            "fetch_time": datetime.now(timezone.utc).isoformat(),
            "title": f"{company_overview.get('company_name')} 公司概况",
        },
    ]
    dual_sources = market_snapshot.get("dual_sources") or {}
    east_quote = dual_sources.get("eastmoney")
    tdx_quote = dual_sources.get("tdx")
    if east_quote and east_quote.get("last_price") is not None:
        citations.append(
            {
                "source_name": "eastmoney 实时行情(双源)",
                "source_url": "https://quote.eastmoney.com",
                "fetch_time": east_quote.get("fetch_time"),
                "title": f"{stock_code} 东财价 {east_quote.get('last_price')}",
                "relevance_score": 1.0,
                "timeliness_hours": _timeliness_hours(east_quote.get("fetch_time")),
            }
        )
    if tdx_quote and tdx_quote.get("last_price") is not None:
        citations.append(
            {
                "source_name": "tdx(mootdx) 实时行情(双源)",
                "source_url": "https://github.com/mootdx/mootdx",
                "fetch_time": tdx_quote.get("fetch_time"),
                "title": f"{stock_code} 通达信价 {tdx_quote.get('last_price')}",
                "relevance_score": 1.0,
                "timeliness_hours": _timeliness_hours(tdx_quote.get("fetch_time")),
            }
        )

    for x in social_items:
        citations.append(
            {
                "source_name": f"{x.get('platform')} 热搜",
                "source_url": x.get("source_url"),
                "fetch_time": x.get("fetch_time").isoformat() if hasattr(x.get("fetch_time"), "isoformat") else str(x.get("fetch_time")),
                "title": x.get("title"),
                "relevance_score": x.get("relevance_score"),
                "timeliness_hours": x.get("timeliness_hours"),
            }
        )

    for x in news_items + policy_items:
        c = dict(x)
        c.setdefault("relevance_score", 0.7)
        c.setdefault("timeliness_hours", _timeliness_hours(c.get("fetch_time")))
        citations.append(c)

    citations.extend(
        [
            {
                "source_name": "东方财富-龙虎榜",
                "source_url": "https://data.eastmoney.com/stock/lhb.html",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "title": f"{stock_code} 龙虎榜维度",
                "relevance_score": 0.8,
            },
            {
                "source_name": "东方财富-融资融券",
                "source_url": "https://data.eastmoney.com/rzrq/",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "title": f"{stock_code} 两融维度",
                "relevance_score": 0.8,
            },
            {
                "source_name": "东方财富-资金流向",
                "source_url": "https://data.eastmoney.com/zjlx/",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "title": f"{stock_code} 主力资金流维度",
                "relevance_score": 0.8,
            },
        ]
    )

    citations = [c for c in citations if str(c.get("source_url", "")).startswith("http")]

    # E3：市场状态过滤 —— 熊市下 B/C 策略 BUY 信号不生成 sim 指令
    _market_state = get_cached_market_state()
    _strategy_type = str((price_forecast.get("readiness") or {}).get("stability_grade") or "B").upper()
    _filtered_out_e3 = (
        recommendation == "BUY"
        and _market_state == "BEAR"
        and _strategy_type in ("B", "C")
    )
    _filter_reason_e3 = "BEAR市况下策略类型B/C被过滤" if _filtered_out_e3 else None

    f = market_features.get("features", {})
    rt_analysis_steps = [
        f"步骤1-公司概况：{company_overview.get('company_name')}，行业={company_overview.get('industry')}，上市日期={company_overview.get('listed_date')}。",
        f"步骤2-行情解析：读取最新价={market_snapshot.get('last_price')}，涨跌幅={market_snapshot.get('pct_change')}，双源状态={market_snapshot.get('dual_status')}，价差={((market_snapshot.get('dual_comparison') or {}).get('last_price_diff_pct'))}。",
        f"步骤3-技术面：MA5={f.get('ma5')}，MA20={f.get('ma20')}，趋势={f.get('trend')}，5日收益={f.get('ret5')}。",
        (lambda tf=tdx_local.get('features', {}): (
            f"步骤4-本地因子：样本天数={'暂无' if tf.get('sample_days') is None else tf.get('sample_days')}，"
            f"20日波动={'暂无' if tf.get('volatility20') is None else round(tf.get('volatility20')*100, 3)}%，"
            f"3年历史分位={'暂无' if tf.get('price_percentile_3y') is None else round(tf.get('price_percentile_3y')*100, 1)}%。"
        ))(),
        (lambda mf=((capital_dims.get('capital_flow') or {}).get('main_force') or {}),
                mg=(capital_dims.get('margin_financing') or {}),
                lhb=(capital_dims.get('dragon_tiger') or {}),
                nb=((capital_dims.get('capital_flow') or {}).get('northbound') or {}):
            (
                f"步骤5-资金博弈：主力5日净流={'暂无' if mf.get('net_inflow_5d') is None else _fmt_yuan(mf.get('net_inflow_5d'))}{'（代理估算）' if mf.get('proxy') else ''}，"
                f"北向5日净流={'暂无' if nb.get('status') != 'ok' or nb.get('net_inflow_5d') is None else _fmt_yuan(nb.get('net_inflow_5d'))}，龙虎榜30日次数={lhb.get('lhb_count_30d') if lhb.get('lhb_count_30d') is not None else '—'}，"
                f"两融5日变化={'暂无' if mg.get('rzye_delta_5d') is None else _fmt_yuan(mg.get('rzye_delta_5d'))}{'（代理估算，与主力流向同源）' if mg.get('proxy') else ''}。"
            )
        )(),
        f"步骤6-外部信息：筛入社媒={len(social_items)}条（相关性阈值>={settings.hotspot_relevance_threshold}，时效<={settings.hotspot_max_age_hours}h），新闻={len(news_items)}条，政策={len(policy_items)}条，市场宽度={'暂无' if not isinstance(market_breadth, dict) or market_breadth.get('breadth_score') is None else market_breadth.get('breadth_score')}。",
        "步骤7-模型与规则交叉：模型建议与规则建议一致则采用，不一致则降为观望并标记冲突。",
        "步骤8-方向预测：基于择优信号模型输出1~7天方向（上涨/下跌/震荡）与持有/卖出动作。",
        "步骤9-回验计划：至少对近3个月历史样本执行滚动回测，并按1~7日窗口结算方向正确性，同时保留全历史稳定性参考。",
    ]

    # 高级区：本报告所用数据（§5 透明化）
    def _data_source_row(name: str, time_range: str, record_count: int, status: str, status_reason: str = "") -> dict:
        return {"name": name, "time_range": time_range, "record_count": record_count, "status": status, "status_reason": status_reason or ""}

    report_data_usage_sources = []
    report_data_usage_sources.append(
        _data_source_row(
            "东方财富行情",
            "当日",
            1 if market_snapshot.get("last_price") is not None and market_snapshot.get("source") == "eastmoney" else 0,
            "ok" if market_snapshot.get("source") == "eastmoney" and market_snapshot.get("last_price") is not None else ("degraded" if market_snapshot.get("last_price") is not None else "missing"),
            "" if market_snapshot.get("last_price") is not None else "未获取到东财行情",
        )
    )
    report_data_usage_sources.append(
        _data_source_row(
            "通达信(mootdx)行情",
            "当日",
            1 if (market_snapshot.get("dual_sources") or {}).get("tdx", {}).get("last_price") is not None else 0,
            "ok" if (market_snapshot.get("dual_sources") or {}).get("tdx", {}).get("last_price") is not None else "missing",
            "" if (market_snapshot.get("dual_sources") or {}).get("tdx", {}).get("last_price") is not None else "通达信数据未接入或不可用",
        )
    )
    report_data_usage_sources.append(
        _data_source_row(
            "公司概况与估值",
            "当日",
            1 if company_overview.get("company_name") or valuation.get("pe_ttm") is not None else 0,
            "ok" if (company_overview.get("company_name") or valuation.get("pe_ttm") is not None) else "degraded",
            "" if company_overview.get("company_name") else "部分字段缺失",
        )
    )
    report_data_usage_sources.append(
        _data_source_row(
            "微博/抖音热搜",
            "近72小时",
            len(social_items),
            "ok" if len(social_items) > 0 else "missing",
            "" if len(social_items) > 0 else "未采集到相关热搜或超时",
        )
    )
    report_data_usage_sources.append(
        _data_source_row("新闻与公告", "近7日", len(news_items), "ok" if len(news_items) > 0 else "stale_ok", "" if len(news_items) > 0 else "当日无新闻时使用近期缓存")
    )
    report_data_usage_sources.append(
        _data_source_row("政策信息", "近7日", len(policy_items), "ok" if len(policy_items) > 0 else "stale_ok", "" if len(policy_items) > 0 else "当日无政策时使用近期缓存")
    )
    cap = capital_dims.get("capital_flow", {}) or {}
    mf = cap.get("main_force") or {}
    report_data_usage_sources.append(
        _data_source_row(
            "主力资金",
            "近5日",
            mf.get("record_count", 1) if mf.get("status") == "ok" else 0,
            mf.get("status", "missing"),
            mf.get("reason", "") or ("" if mf.get("status") == "ok" else "数据不可用"),
        )
    )
    dg = (capital_dims.get("dragon_tiger") or {})
    report_data_usage_sources.append(
        _data_source_row("龙虎榜", "近30日", dg.get("lhb_count_30d", 0) or 0, dg.get("status", "missing"), dg.get("reason", "") or ("" if dg.get("status") == "ok" else "未上榜或数据不可用"))
    )
    mg = (capital_dims.get("margin_financing") or {})
    report_data_usage_sources.append(
        _data_source_row("两融数据", "近5/20日", 1 if mg.get("status") == "ok" else 0, mg.get("status", "missing"), mg.get("reason", "") or ("" if mg.get("status") == "ok" else "数据不可用"))
    )

    report_json = {
        "report_id": report_id,
        "stock_code": stock_code,
        "trade_date": effective_trade_date,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "recommendation": recommendation,
        "recommendation_cn": {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(recommendation, "观望等待"),
        "analysis_summary_cn": cn_summary,
        "novice_guide": novice_guide,
        "plain_report": plain_report,
        "quality_gate": quality_gate,
        "report_data_usage": {"sources": report_data_usage_sources},
        "company_overview": {
            "company_name": company_overview.get("company_name"),
            "stock_code": company_overview.get("stock_code"),
            "industry": company_overview.get("industry"),
            "industry_csrc": company_overview.get("industry_csrc"),
            "exchange": company_overview.get("exchange"),
            "market_type": company_overview.get("market_type"),
            "listed_date": company_overview.get("listed_date"),
            "legal_person": company_overview.get("legal_person"),
            "chairman": company_overview.get("chairman"),
            "website": company_overview.get("website"),
            "employees": company_overview.get("employees"),
            "intro": company_overview.get("intro"),
            "business_scope": company_overview.get("business_scope"),
        },
        "industry_competition": {
            "industry_name": industry_competition.get("industry_name"),
            "industry_board_code": industry_competition.get("industry_board_code"),
            "industry_board_name": industry_competition.get("industry_board_name"),
            "peers": industry_competition.get("peers") or [],
        },
        "financial_analysis": {
            "total_shares": valuation.get("total_shares"),
            "float_shares": valuation.get("float_shares"),
            "total_market_cap": valuation.get("total_market_cap"),
            "float_market_cap": valuation.get("float_market_cap"),
            "ret5": market_features.get("features", {}).get("ret5"),
            "ret20": market_features.get("features", {}).get("ret20"),
            "sample_days": market_features.get("features", {}).get("sample_days"),
            "tdx_local_features": tdx_local.get("features", {}),
        },
        "valuation": {
            "pe_ttm": valuation.get("pe_ttm"),
            "pb": valuation.get("pb"),
            "listed_days": _days_since_listed(company_overview.get("listed_date")),
            "market_cap": valuation.get("total_market_cap"),
            "region": valuation.get("region"),
            "industry": valuation.get("industry"),
        },
        "indicator_explanation": {
            "ma5_desc": "五日均线（MA5）为近5个交易日收盘价均值，用于观察短期趋势。",
            "ma20_desc": "二十日均线（MA20）为近20个交易日收盘价均值，用于观察中期趋势。",
        },
        "price_forecast": price_forecast,
        "direction_forecast": direction_forecast,
        "dimensions": {
            "market_state": _market_state,
            "market_snapshot": market_snapshot,
            "market_dual_source": {
                "dual_status": market_snapshot.get("dual_status"),
                "selected_by_order": market_snapshot.get("source_selected_by_order"),
                "comparison": market_snapshot.get("dual_comparison"),
                "sources": market_snapshot.get("dual_sources"),
            },
            "market_features": market_features,
            "social_trend": {
                "topic_count": len(social_items),
                "missing_reason": "未抓到满足相关性和时效阈值的热搜" if len(social_items) == 0 else None,
                "top_topics": [
                    {
                        "title": x.get("title"),
                        "platform": x.get("platform"),
                        "source_url": x.get("source_url"),
                        "relevance_score": x.get("relevance_score"),
                        "timeliness_hours": x.get("timeliness_hours"),
                        "match_method": x.get("match_method"),
                    }
                    for x in social_items
                ],
            },
            "news_policy": {
                "news_count": len(news_items),
                "policy_count": len(policy_items),
            },
            "market_breadth": market_breadth,
            "capital_flow": capital_dims.get("capital_flow"),
            "dragon_tiger": capital_dims.get("dragon_tiger"),
            "margin_financing": capital_dims.get("margin_financing"),
            "capital_errors": capital_dims.get("errors") or [],
        },
        "thesis": {
            "social_factor_weight": 0.2,
            "event_conflict_flag": llm_reco != rule_reco,
            "degraded_mode": degraded_mode,
            "direction_override": direction_override,
            "market_state": _market_state,
            "filtered_out": _filtered_out_e3,
            "filter_reason": _filter_reason_e3,
            # Derived from LLM-parsed fields; non-empty only if LLM provided them.
            # Trigger/invalidation: split only on；or explicit "，或" boundary to avoid
            # cutting Chinese conjunctions (，) inside a single condition description.
            "trigger_conditions": [s.strip() for s in re.split(r'[；;]|，或', llm_trigger) if s.strip()] if llm_trigger else [],
            "invalidation_conditions": [s.strip() for s in re.split(r'[；;]|，或', llm_invalidation) if s.strip()] if llm_invalidation else [],
            # investment_points: strip 【7日预测依据】prefix before splitting
            "investment_points": (lambda t: [
                s.strip() for s in re.split(r'[；;]', re.sub(r'^【[^】]*】[^。]*。', '', t).strip()) if s.strip()
            ][:4])(llm_reason_text) if llm_reason_text else [],
            "key_risks": [
                # Strip leading ordinal markers like "1）", "2.", "（1）", "（一）", "）" etc.
                re.sub(r'^[（(]?[\d０-９一二三四五六七八九十]*[）).、．\)]\s*', '', s.strip().lstrip("0123456789. （(）)"))
                for s in re.split(r'[；;]', llm_risks) if s.strip()
            ] if llm_risks else [],
        },
        "citations": citations,
        "reasoning_trace": {
            "data_sources": ["实时行情", "公司概况", "行业板块", "K线特征", "龙虎榜", "资金流", "融资融券", "社媒热搜", "新闻", "政策", "模型推理"],
            "evidence_items": _build_reasoning_evidence(citations),
            "analysis_steps": rt_analysis_steps,
            "inference_summary": summary,
            "validation_plan": {
                "windows": [1, 2, 3, 4, 5, 6, 7],
                "minimum_backtest_trading_days": 63,
                "fields": ["actual_return", "direction_correct"],
                "settle_schedule": "每个窗口到期后自动结算",
            },
            "raw_inputs": _jsonable(
                {
                    "market_snapshot": market_snapshot,
                    "market_features": market_features,
                    "social_items": social_items,
                    "news_items": news_items,
                    "policy_items": policy_items,
                    "company_overview": company_overview,
                    "industry_competition": industry_competition,
                    "valuation": valuation,
                    "tdx_local_features": tdx_local,
                    "market_breadth": market_breadth,
                    "capital_flow": capital_dims.get("capital_flow"),
                    "dragon_tiger": capital_dims.get("dragon_tiger"),
                    "margin_financing": capital_dims.get("margin_financing"),
                    "capital_errors": capital_dims.get("errors") or [],
                }
            ),
        },
        "outcome_verification": [],
    }

    # Compute aggregate confidence from LLM 7d window
    _w7_for_conf = next((w for w in (price_forecast.get("windows") or []) if w.get("horizon_days") == 7), None)
    _agg_confidence = max(0.40, float((_w7_for_conf or {}).get("llm_confidence") or 0.55))

    # E2 三方投票审计：步骤 5，在置信度校准前、主分析完成后（03 详细设计 §4、13 §10）
    _audit_flag = "not_triggered"
    _audit_detail = ""
    _audit_skip_reason: str | None = None
    _contradiction = str((_llm_json_parsed or {}).get("contradiction") or "无").strip()
    if (report_json.get("thesis") or {}).get("event_conflict_flag"):
        _contradiction = _contradiction if _contradiction != "无" else "模型与规则结论不一致"
    if not settings.mock_llm:
        from app.services.llm_router import should_trigger_audit, run_audit_and_aggregate

        if should_trigger_audit(recommendation, _agg_confidence, _contradiction):
            try:
                _report_summary = (cn_summary or "")[:400]
                _audit_result = await run_audit_and_aggregate(
                    main_vote=recommendation,
                    base_confidence=_agg_confidence,
                    report_summary=_report_summary,
                    timeout_sec=90,
                )
                if _audit_result.get("audit_flag") == "audit_skipped":
                    _audit_flag = "audit_skipped"
                    _audit_detail = ""
                    _audit_skip_reason = _audit_result.get("skip_reason") or "audit_call_failed"
                else:
                    _audit_flag = _audit_result.get("audit_flag") or "majority_agree"
                    _audit_detail = _audit_result.get("audit_detail") or ""
                    _agg_confidence = float(_audit_result.get("adjusted_confidence") or _agg_confidence)
                    recommendation = _audit_result.get("final_recommendation") or recommendation
                    report_json["recommendation"] = recommendation
                    report_json["recommendation_cn"] = {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(recommendation, "观望等待")
            except Exception as _audit_err:
                logger.warning("audit_run_failed recommendation=%s: %s", recommendation, _audit_err)
                _audit_flag = "audit_skipped"
                _audit_skip_reason = f"exception:{_audit_err!s}"
    else:
        if (  # 即使 mock_llm，若满足触发条件也写入 audit_flag，便于 E2E 用例 mock
            (recommendation == "BUY" or _agg_confidence >= settings.sim_instruction_confidence_threshold or _contradiction != "无")
        ):
            _audit_flag = "audit_skipped"
            _audit_skip_reason = "mock_llm"
    report_json["audit_flag"] = _audit_flag
    report_json["audit_detail"] = _audit_detail
    if _audit_skip_reason:
        report_json["audit_skip_reason"] = _audit_skip_reason

    # 模拟实盘 §3.5：BUY 强信号生成 sim_trade_instruction 并注入 content_json
    _filtered_out = (report_json.get("thesis") or {}).get("filtered_out", False)
    _should_instruction = (
        recommendation == "BUY"
        and _agg_confidence >= settings.sim_instruction_confidence_threshold
        and not _filtered_out
    )
    if _should_instruction:
        _close = market_snapshot.get("last_price")
        if isinstance(_close, (int, float)) and _close > 0:
            from app.services.sim_settle_service import HOLD_DAYS_MAX
            from app.services.trade_calendar import next_trade_date_str
            _stype = "B"
            _sim_open = round(_close * 1.025, 2)
            _stop = round(_close * 0.92, 2)
            _atr_risk = _close - _stop
            _t1 = round(min(_close + _atr_risk * 1.5, _close * 1.30), 2)
            _t2 = round(min(_close + _atr_risk * 2.5, _close * 1.50), 2)
            _hold_max = HOLD_DAYS_MAX.get(_stype, 15)
            from datetime import date, timedelta
            _sim_open_d = next_trade_date_str(effective_trade_date)
            try:
                _d = date.fromisoformat(_sim_open_d)
                _valid = (_d + timedelta(days=_hold_max + 5)).isoformat()
            except Exception:
                _valid = "9999-12-31"
            _stock_name = (company_overview.get("short_name") or company_overview.get("company_name")) if isinstance(company_overview, dict) else stock_code
            report_json["sim_trade_instruction"] = {
                "sim_open_price": _sim_open,
                "stop_loss_price": _stop,
                "target_price_1": _t1,
                "target_price_2": _t2,
                "strategy_type": _stype,
                "sim_qty": 100,
                "valid_until": _valid,
                "stock_name": _stock_name,
            }
    row = Report(
        report_id=report_id,
        stock_code=stock_code,
        run_mode=run_mode,
        recommendation=recommendation,
        confidence=_agg_confidence,
        content_json=report_json,
        trade_date=effective_trade_date,
        source=source or "real",
    )
    db.add(row)
    db.add(
        ModelRunLog(
            request_id=report_id,
            model_version=llm["model"],
            prompt_version="v6",
            latency_ms=llm["latency_ms"],
            token_in=0,
            token_out=0,
            status="ok",
        )
    )
    db.commit()
    db.refresh(row)

    # 报告只读与留痕：发布/降级等关键动作用日志留痕（整合文档 §6、05 验收条款 35）
    qg = report_json.get("quality_gate") or {}
    logger.info(
        "report_published report_id=%s stock_code=%s trade_date=%s publish_decision=%s",
        row.report_id,
        stock_code,
        effective_trade_date,
        qg.get("publish_decision", "unknown"),
    )

    # 模拟实盘 §3.5：BUY 强信号 + 无 HALT → 创建 sim_position
    _instr = report_json.get("sim_trade_instruction")
    if _instr and recommendation == "BUY" and _agg_confidence >= settings.sim_instruction_confidence_threshold and not _filtered_out:
        from app.models import SimAccount
        from app.services.sim_position_service import create_position
        _latest = (
            db.query(SimAccount)
            .filter(SimAccount.capital_tier == "10w")
            .order_by(SimAccount.snapshot_date.desc())
            .first()
        )
        _dd_state = _latest.drawdown_state if _latest else "NORMAL"
        if _dd_state != "HALT":
            try:
                create_position(
                    db,
                    report_id=row.report_id,
                    stock_code=stock_code,
                    stock_name=_instr.get("stock_name"),
                    signal_date=effective_trade_date,
                    instruction=_instr,
                    capital_tier="10w",
                )
                db.commit()
                from app.services.notification import send_admin_notification
                send_admin_notification(
                    "buy_signal",
                    {
                        "report_id": row.report_id,
                        "stock_code": stock_code,
                        "stock_name": _instr.get("stock_name"),
                    },
                )
            except Exception as e:
                logger.warning("sim_position_create_failed report_id=%s err=%s", row.report_id, e)
                db.rollback()

    if idempotency_key:
        db.merge(
            ReportIdempotency(
                idempotency_key=idempotency_key,
                stock_code=stock_code,
                run_mode=run_mode,
                report_id=row.report_id,
            )
        )
        db.commit()

    return (row, False)


async def settle_prediction(db: Session, report_id: str, stock_code: str, windows: list[int]):
    report = db.get(Report, report_id)
    recommendation = report.recommendation if report else "HOLD"
    trade_date = (report.trade_date or "").strip() if report else ""
    outcomes = []
    verification = []

    for w in windows:
        # 按报告交易日起算 w 日实际收益，避免用「最近 w 日」导致窗口错位
        actual = await fetch_price_return_from_trade_date(stock_code, trade_date, w)
        is_correct, err_type = _judge_correct(recommendation, actual)
        row = PredictionOutcome(
            report_id=report_id,
            stock_code=stock_code,
            window_days=w,
            actual_result=actual,
            is_correct=is_correct,
            error_type=err_type,
            settled_at=datetime.now(timezone.utc),
        )
        db.add(row)
        outcomes.append(row)
        verification.append({"window_days": w, "actual_result": actual, "is_correct": is_correct, "error_type": err_type})

    if report:
        content = dict(report.content_json)
        content["outcome_verification"] = verification
        report.content_json = content
        db.merge(report)

    db.commit()
    return outcomes


def prediction_stats(db: Session):
    rows = db.query(PredictionOutcome).filter(PredictionOutcome.is_correct.isnot(None)).all()
    total_judged = len(rows)
    correct = len([x for x in rows if x.is_correct == 1])
    acc = (correct / total_judged) if total_judged else 0.0

    by_window: list[dict] = []
    for w in [1, 7, 14, 30, 60]:
        sub = [x for x in rows if x.window_days == w]
        n = len(sub)
        hit = len([x for x in sub if x.is_correct == 1])
        by_window.append({
            "window_days": w,
            "accuracy": round(hit / n, 4) if n else None,
            "samples": n,
            "coverage": round(n / max(1, total_judged), 4) if total_judged else 0,
        })

    from collections import defaultdict
    stock_agg = defaultdict(lambda: {"correct": 0, "total": 0})
    for x in rows:
        stock_agg[x.stock_code]["total"] += 1
        if x.is_correct == 1:
            stock_agg[x.stock_code]["correct"] += 1
    by_stock = [
        {"stock_code": sc, "accuracy": round(d["correct"] / d["total"], 4) if d["total"] else 0, "samples": d["total"]}
        for sc, d in sorted(stock_agg.items())
    ]

    cutoff = datetime.now(timezone.utc) - timedelta(days=92)
    def _aware(dt):
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    recent = [x for x in rows if x.settled_at and _aware(x.settled_at) >= cutoff]
    recent_n = len(recent)
    recent_hit = len([x for x in recent if x.is_correct == 1])
    recent_3m = {
        "accuracy": round(recent_hit / recent_n, 4) if recent_n else None,
        "samples": recent_n,
        "coverage": round(recent_n / max(1, total_judged), 4) if total_judged else 0,
        "start_date": (cutoff + timedelta(days=2)).strftime("%Y-%m-%d"),
        "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }

    fb_cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    fb_rows = db.query(ReportFeedback).filter(ReportFeedback.created_at >= fb_cutoff).all()
    fb_total = len(fb_rows)
    fb_negative = sum(1 for r in fb_rows if r.is_helpful == 0)
    negative_feedback_rate = round(fb_negative / fb_total, 4) if fb_total else None

    # by_strategy_type aggregation
    strategy_agg: dict[str, dict] = {}
    for x in rows:
        st = getattr(x, "strategy_type", None) or "A"
        if st not in strategy_agg:
            strategy_agg[st] = {"correct": 0, "total": 0, "pnl_values": []}
        strategy_agg[st]["total"] += 1
        if x.is_correct == 1:
            strategy_agg[st]["correct"] += 1
        pv = getattr(x, "pnl", None)
        if pv is not None:
            strategy_agg[st]["pnl_values"].append(pv)
    by_strategy_type: dict[str, dict] = {}
    for st, d in sorted(strategy_agg.items()):
        n = d["total"]
        wins = d["correct"]
        pnls = d["pnl_values"]
        losses = [p for p in pnls if p < 0]
        avg_win = sum(p for p in pnls if p > 0) / max(1, wins) if wins else 0
        avg_loss = abs(sum(losses) / len(losses)) if losses else 1
        by_strategy_type[st] = {
            "accuracy": round(wins / n, 4) if n else None,
            "total": n,
            "coverage": round(n / max(1, total_judged), 4) if total_judged else 0,
            "pnl_ratio": round(avg_win / max(avg_loss, 0.0001), 2) if (avg_win or avg_loss) else None,
        }

    return {
        "total_judged": total_judged,
        "judged": total_judged,
        "accuracy": round(acc, 4),
        "by_window": by_window,
        "by_stock": by_stock,
        "recent_3m": recent_3m,
        "negative_feedback_rate": negative_feedback_rate,
        "negative_feedback_total": fb_total,
        "by_strategy_type": by_strategy_type,
        "alpha": None,
    }


def collect_topics(db: Session, stock_code: str, raw_topics: list[dict], stock_name: str | None = None):
    industry = _industry_keywords(stock_code)
    for topic in raw_topics:
        if not str(topic.get("source_url", "")).startswith("http"):
            continue

        link = link_topic_to_stock(topic["title"], stock_code, stock_name=stock_name, industry_keywords=industry)
        if link["relevance_score"] < settings.hotspot_relevance_threshold:
            continue

        db.add(
            HotspotRaw(
                platform=topic["platform"],
                rank=topic["rank"],
                title=topic["title"],
                raw_heat=topic["raw_heat"],
                fetch_time=topic["fetch_time"],
                source_url=topic["source_url"],
                cookie_version="v1",
            )
        )
        norm = enrich_topic(topic)
        db.merge(HotspotNormalized(**norm))
        db.add(
            HotspotStockLink(
                topic_id=topic["topic_id"],
                stock_code=stock_code,
                relevance_score=link["relevance_score"],
                match_method=link["match_method"],
            )
        )
        # Write to SSOT tables
        from uuid import uuid4 as _uuid4
        hotspot_item_id = str(_uuid4())
        db.add(
            MarketHotspotItem(
                hotspot_item_id=hotspot_item_id,
                batch_id=str(_uuid4()),
                source_name=topic.get("platform", "unknown"),
                merged_rank=topic.get("rank", 999),
                source_rank=topic.get("rank"),
                topic_title=topic["title"],
                source_url=topic.get("source_url", ""),
                fetch_time=topic.get("fetch_time"),
            )
        )
        db.add(
            MarketHotspotItemStockLink(
                hotspot_item_stock_link_id=str(_uuid4()),
                hotspot_item_id=hotspot_item_id,
                stock_code=stock_code,
                relation_role="primary",
                match_confidence=link["relevance_score"],
            )
        )
    db.commit()


def run_regression(db: Session):
    stats = prediction_stats(db)
    baseline = {"accuracy": 0.5}
    decision = "publish" if stats["accuracy"] >= baseline["accuracy"] else "rollback"
    row = EnhancementExperiment(
        name=f"exp-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        baseline_metrics=baseline,
        candidate_metrics=stats,
        decision=decision,
        notes="auto-evaluated",
    )
    db.add(row)
    db.commit()
    return row
