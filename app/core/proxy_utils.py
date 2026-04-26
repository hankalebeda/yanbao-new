"""代理环境工具：北向/ETF 等访问国内站点时需绕过系统代理"""
from __future__ import annotations

import os


def bypass_proxy() -> None:
    """
    绕过系统代理直连国内站点（东方财富、交易所等）。
    本机使用代理（如 Clash、V2Ray）时，对国内站点易报 ConnectionResetError，
    需在 akshare/requests 调用前执行。
    与 capital_flow、market_data 的 trust_env=False 策略一致。
    """
    os.environ["NO_PROXY"] = "*"
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)
