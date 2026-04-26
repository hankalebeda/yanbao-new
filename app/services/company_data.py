from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx


def _to_secid(stock_code: str) -> str:
    code = stock_code.split(".")[0]
    return f"1.{code}" if code.startswith("6") else f"0.{code}"


def _fmt_date(v: Any) -> str | None:
    s = str(v or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "-", ""):
            return None
        return float(v)
    except Exception:
        return None


_industry_cache: dict[str, Any] = {"ts": None, "items": []}


async def fetch_company_overview(stock_code: str) -> dict:
    code = stock_code.split(".")[0]
    code_full = f"SH{code}" if code.startswith("6") else f"SZ{code}"
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://emweb.securities.eastmoney.com/",
    }
    async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
        try:
            r = await client.get(url, params={"code": code_full}, headers=headers)
            r.raise_for_status()
            payload = r.json()
        except Exception:
            payload = {}

    jbzl = payload.get("jbzl") or {}
    fxxg = payload.get("fxxg") or {}
    result = {
        "company_name": jbzl.get("gsmc") or jbzl.get("agjc") or stock_code,
        "stock_code": stock_code,
        "english_name": jbzl.get("ywmc"),
        "short_name": jbzl.get("agjc"),
        "industry": jbzl.get("sshy"),
        "industry_csrc": jbzl.get("sszjhhy"),
        "exchange": jbzl.get("ssjys"),
        "market_type": jbzl.get("zqlb"),
        "listed_date": _fmt_date(jbzl.get("ssrq") or fxxg.get("ssrq")),
        "legal_person": jbzl.get("frdb"),
        "chairman": jbzl.get("dsz"),
        "website": jbzl.get("gswz"),
        "contact_phone": jbzl.get("lxdh"),
        "email": jbzl.get("dzxx"),
        "employees": jbzl.get("gyrs"),
        "intro": jbzl.get("gsjj"),
        "business_scope": jbzl.get("jyfw"),
        "raw": {
            "company_survey": jbzl,
            "issuance": fxxg,
        },
    }
    if result["company_name"] == stock_code or not result.get("industry"):
        try:
            import akshare as ak  # type: ignore

            code = stock_code.split(".")[0]
            df = ak.stock_individual_info_em(symbol=code)
            info = {str(x["item"]): x["value"] for _, x in df.iterrows()}
            result["company_name"] = info.get("股票简称") or result["company_name"]
            result["industry"] = info.get("行业") or result.get("industry")
            result["listed_date"] = _fmt_date(info.get("上市时间")) or result.get("listed_date")
            result["raw"]["akshare_info"] = info
        except Exception:
            pass
    return result


async def fetch_valuation_snapshot(stock_code: str) -> dict:
    secid = _to_secid(stock_code)
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    fields = "f57,f58,f84,f85,f86,f116,f117,f9,f23,f127,f128,f129"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com"}
    async with httpx.AsyncClient(timeout=15, headers=headers, trust_env=False) as client:
        try:
            r = await client.get(url, params={"secid": secid, "fields": fields})
            r.raise_for_status()
            d = r.json().get("data") or {}
        except Exception:
            d = {}

    # Fallback to akshare for basic fields when eastmoney quote endpoint is unavailable.
    if not d:
        try:
            import akshare as ak  # type: ignore

            code = stock_code.split(".")[0]
            df = ak.stock_individual_info_em(symbol=code)
            info = {str(x["item"]): x["value"] for _, x in df.iterrows()}
            d = {
                "f58": info.get("股票简称"),
                "f84": info.get("总股本"),
                "f85": info.get("流通股"),
                "f116": info.get("总市值"),
                "f117": info.get("流通市值"),
                "f127": info.get("行业"),
            }
        except Exception:
            d = {}

    def _pe_pb(v: Any) -> float | None:
        """东方财富 fltt=2 模式下，PE/PB 字段以整数形式返回放大100倍的值（如 2132 → 21.32）。
        需要除以100才能得到真实的倍数。市值/股本字段单位为元，不需要换算。
        """
        raw = _safe_float(v)
        if raw is None:
            return None
        return round(raw / 100, 4)

    return {
        "stock_code": stock_code,
        "stock_name": d.get("f58"),
        "industry": d.get("f127"),
        "region": d.get("f128"),
        "concepts": [x for x in str(d.get("f129") or "").split(",") if x],
        "pe_ttm": _pe_pb(d.get("f9")),    # 东财 fltt=2 下需要 ÷100
        "pb": _pe_pb(d.get("f23")),        # 东财 fltt=2 下需要 ÷100
        "total_market_cap": _safe_float(d.get("f116")),
        "float_market_cap": _safe_float(d.get("f117")),
        "total_shares": _safe_float(d.get("f84")),
        "float_shares": _safe_float(d.get("f85")),
        "listed_days": d.get("f86"),
        "fetch_time": datetime.now(timezone.utc).isoformat(),
        "raw": d,
    }


async def _load_industry_boards(client: httpx.AsyncClient) -> list[dict]:
    now = datetime.now(timezone.utc)
    ts = _industry_cache.get("ts")
    if ts and isinstance(ts, datetime) and (now - ts).total_seconds() < 6 * 3600:
        return _industry_cache.get("items") or []

    items: list[dict] = []
    for pn in range(1, 10):
        r = await client.get(
            "https://push2.eastmoney.com/api/qt/clist/get",
            params={
                "pn": str(pn),
                "pz": "100",
                "po": "1",
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "fid": "f12",
                "fs": "m:90+t:2",
                "fields": "f12,f14",
            },
        )
        r.raise_for_status()
        diff = (r.json().get("data") or {}).get("diff") or []
        if not diff:
            break
        items.extend(diff)

    _industry_cache["ts"] = now
    _industry_cache["items"] = items
    return items


def _norm_text(v: str | None) -> str:
    return str(v or "").strip().replace(" ", "")


async def fetch_industry_competition(stock_code: str, industry_name: str | None) -> dict:
    code = stock_code.split(".")[0]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/",
    }
    async with httpx.AsyncClient(timeout=20, headers=headers, trust_env=False) as client:
        board_code = None
        board_name = None
        board_raw = None
        try:
            boards = await _load_industry_boards(client)
            target = _norm_text(industry_name)
            for b in boards:
                n = _norm_text(b.get("f14"))
                if n == target:
                    board_code = b.get("f12")
                    board_name = b.get("f14")
                    board_raw = b
                    break
            if not board_code and target:
                for b in boards:
                    n = _norm_text(b.get("f14"))
                    if target in n or n in target:
                        board_code = b.get("f12")
                        board_name = b.get("f14")
                        board_raw = b
                        break
            if not board_code and ("酿酒" in target or "白酒" in target):
                board_code = "BK1277"
                board_name = "白酒Ⅱ"
                board_raw = {"f12": "BK1277", "f14": "白酒Ⅱ", "match": "heuristic"}
        except Exception:
            boards = []

        peers: list[dict] = []
        peers_raw: list[dict] = []
        if board_code:
            try:
                r = await client.get(
                    "https://push2.eastmoney.com/api/qt/clist/get",
                    params={
                        "pn": "1",
                        "pz": "30",
                        "po": "1",
                        "np": "1",
                        "fltt": "2",
                        "invt": "2",
                        "fid": "f116",
                        "fs": f"b:{board_code}+f:!50",
                        "fields": "f12,f14,f2,f3,f9,f23,f116",
                    },
                )
                r.raise_for_status()
                peers_raw = (r.json().get("data") or {}).get("diff") or []
                for x in peers_raw:
                    if str(x.get("f12")) == code:
                        continue
                    def _pe_pb_peer(v: Any) -> float | None:
                        raw = _safe_float(v)
                        return round(raw / 100, 4) if raw is not None else None

                    peers.append(
                        {
                            "stock_code": x.get("f12"),
                            "stock_name": x.get("f14"),
                            "price": _safe_float(x.get("f2")),
                            "pct_change": _safe_float(x.get("f3")),
                            "pe_ttm": _pe_pb_peer(x.get("f9")),   # fltt=2 下需要 ÷100
                            "pb": _pe_pb_peer(x.get("f23")),       # fltt=2 下需要 ÷100
                            "market_cap": _safe_float(x.get("f116")),
                        }
                    )
                    if len(peers) >= 8:
                        break
            except Exception:
                peers = []

    return {
        "industry_name": industry_name,
        "industry_board_code": board_code,
        "industry_board_name": board_name,
        "peers": peers,
        "raw": {
            "board_match": board_raw,
            "peers_raw": peers_raw,
        },
    }
