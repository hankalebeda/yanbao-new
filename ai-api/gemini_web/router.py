"""
FastAPI router for Gemini Web endpoints.

Prefix  : /api/v1/gemini
Tag     : gemini-web
挂载方式 : 在 app/main.py 里 app.include_router(gemini_router)
"""

from fastapi import APIRouter, HTTPException

from app.core.response import envelope

from .client import GeminiWebClient
from .schemas import AnalyzeRequest, BatchRequest

router = APIRouter(prefix="/api/v1/gemini", tags=["gemini-web"])


@router.post(
    "/analyze",
    summary="Gemini 单条分析",
    description=(
        "复用本机已登录的 Chrome 会话，向 Gemini 网页端发送 prompt 并返回回复。\n\n"
        "首次调用会启动后台 Chrome 并复制 Cookie（约 10s），后续复用同一会话。\n\n"
        "**前提**：本机 Chrome 已登录 `gemini.google.com`。"
    ),
)
async def analyze(payload: AnalyzeRequest):
    try:
        client = await GeminiWebClient.get()
        result = await client.analyze(payload.prompt, timeout_ms=payload.timeout_s * 1000)
        return envelope(data=result)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Gemini 调用失败: {exc}")


@router.post(
    "/analyze/batch",
    summary="Gemini 并发批量分析",
    description=(
        "对多只股票并发发送各自 prompt，`asyncio.gather` 同时跑，最多 5 只。\n\n"
        "每只股票独立标签页，互不阻塞；失败项含 `error` 字段，不影响其他结果。"
    ),
)
async def analyze_batch(payload: BatchRequest):
    try:
        client = await GeminiWebClient.get()
        items   = [s.model_dump() for s in payload.stocks]
        results = await client.analyze_batch(items, timeout_ms=payload.timeout_s * 1000)
        return envelope(data={"count": len(results), "results": results})
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Gemini 批量调用失败: {exc}")


@router.delete(
    "/session",
    summary="关闭 Gemini 浏览器会话",
    description="释放后台 Chrome 进程和临时 profile 目录。下次调用 analyze 时自动重新初始化。",
)
async def close_session():
    inst = GeminiWebClient._instance
    if inst:
        await inst.close()
    return envelope(data={"closed": True})


@router.get("/session/status", summary="Gemini session status")
async def session_status():
    inst = GeminiWebClient._instance
    return envelope(
        data={
            "initialized": bool(inst),
            "ready": bool(inst and getattr(inst, "_ready", False)),
        }
    )
