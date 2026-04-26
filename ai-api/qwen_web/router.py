from fastapi import APIRouter, HTTPException

from app.core.response import envelope

from .client import QwenWebClient
from .schemas import AnalyzeRequest, BatchRequest

router = APIRouter(prefix="/api/v1/qwen", tags=["qwen-web"])


@router.post("/analyze", summary="Qwen single analyze")
async def analyze(payload: AnalyzeRequest):
    try:
        client = await QwenWebClient.get()
        result = await client.analyze(payload.prompt, timeout_ms=payload.timeout_s * 1000)
        return envelope(data=result)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Qwen call failed: {exc}")


@router.post("/analyze/batch", summary="Qwen concurrent batch analyze")
async def analyze_batch(payload: BatchRequest):
    try:
        client = await QwenWebClient.get()
        items = [s.model_dump() for s in payload.stocks]
        results = await client.analyze_batch(items, timeout_ms=payload.timeout_s * 1000)
        return envelope(data={"count": len(results), "results": results})
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Qwen batch call failed: {exc}")


@router.delete("/session", summary="Close Qwen browser session")
async def close_session():
    inst = QwenWebClient._instance
    if inst:
        await inst.close()
    return envelope(data={"closed": True})


@router.get("/session/status", summary="Qwen session status")
async def session_status():
    inst = QwenWebClient._instance
    return envelope(
        data={
            "initialized": bool(inst),
            "ready": bool(inst and getattr(inst, "_ready", False)),
        }
    )
