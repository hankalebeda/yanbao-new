import re
from typing import Any

from fastapi import APIRouter, HTTPException

from app.core.response import envelope

from chatgpt_web.client import ChatGPTWebClient
from deepseek_web.client import DeepSeekWebClient
from gemini_web.client import GeminiWebClient
from qwen_web.client import QwenWebClient

from .schemas import AnalyzeRequest, BatchRequest, Provider

router = APIRouter(prefix="/api/v1/webai", tags=["webai-unified"])

_PROVIDER_CLIENTS: dict[Provider, type] = {
    "chatgpt": ChatGPTWebClient,
    "deepseek": DeepSeekWebClient,
    "gemini": GeminiWebClient,
    "qwen": QwenWebClient,
}

_CASE_RE = re.compile(r"CASE-([A-Z0-9_\-]+)\|([A-Z0-9_\-]+)\|OK", re.I)


def _provider_client(provider: Provider):
    return _PROVIDER_CLIENTS[provider]


async def _close_provider_session(provider: Provider) -> None:
    client_cls = _provider_client(provider)
    inst = getattr(client_cls, "_instance", None)
    if inst:
        await inst.close()


async def _run_with_recover(provider: Provider, op):
    client_cls = _provider_client(provider)
    try:
        client = await client_cls.get()
        return await op(client)
    except RuntimeError:
        await _close_provider_session(provider)
        client = await client_cls.get()
        return await op(client)


def _extract_case_tag(text: str) -> str | None:
    m = _CASE_RE.search((text or "").upper())
    if not m:
        return None
    return f"CASE-{m.group(1)}|{m.group(2)}|OK"


def _expected_case_from_prompt(prompt: str) -> str | None:
    return _extract_case_tag(prompt or "")


def _repair_prompt(expected_tag: str) -> str:
    return (
        "你上一条回答未按格式输出。"
        f"现在仅输出这一行，不要任何解释或额外内容：{expected_tag}"
    )


def _normalize_with_expected(result: dict[str, Any], expected_tag: str | None) -> tuple[dict[str, Any], bool]:
    if not expected_tag:
        return result, True
    got = _extract_case_tag(result.get("response", "") or "")
    if got == expected_tag:
        normalized = dict(result)
        normalized["response"] = expected_tag
        return normalized, True
    return result, False


async def _analyze_with_case_retry(client, prompt: str, timeout_ms: int, retries: int = 2) -> dict[str, Any]:
    expected = _expected_case_from_prompt(prompt)
    result = await client.analyze(prompt, timeout_ms=timeout_ms)
    result, ok = _normalize_with_expected(result, expected)
    if ok:
        return result

    if not expected:
        return result

    repaired_prompt = _repair_prompt(expected)
    for _ in range(retries):
        next_result = await client.analyze(repaired_prompt, timeout_ms=timeout_ms)
        next_result, ok = _normalize_with_expected(next_result, expected)
        if ok:
            return next_result
        result = next_result
    return result


def _session_data(provider: Provider) -> dict[str, Any]:
    client_cls = _provider_client(provider)
    inst = getattr(client_cls, "_instance", None)
    data: dict[str, Any] = {
        "provider": provider,
        "initialized": bool(inst),
        "ready": bool(inst and getattr(inst, "_ready", False)),
    }
    if provider == "chatgpt" and inst and hasattr(inst, "model_probe"):
        data["model_probe"] = inst.model_probe
    return data


@router.get("/providers", summary="List supported unified providers")
async def providers():
    return envelope(data={"providers": list(_PROVIDER_CLIENTS.keys())})


@router.post("/analyze", summary="Unified single analyze by provider")
async def analyze(payload: AnalyzeRequest):
    try:
        result = await _run_with_recover(
            payload.provider,
            lambda client: _analyze_with_case_retry(
                client=client,
                prompt=payload.prompt,
                timeout_ms=payload.timeout_s * 1000,
                retries=2,
            ),
        )
        return envelope(data={"provider": payload.provider, **result})
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"{payload.provider} call failed: {exc}")


@router.post("/analyze/batch", summary="Unified batch analyze by provider")
async def analyze_batch(payload: BatchRequest):
    try:
        items = [s.model_dump() for s in payload.stocks]
        initial_results = await _run_with_recover(
            payload.provider,
            lambda client: client.analyze_batch(items, timeout_ms=payload.timeout_s * 1000),
        )
        expected_by_key = {
            (it.get("code", ""), it.get("name", "")): _expected_case_from_prompt(it.get("prompt", ""))
            for it in items
        }
        normalized_results: list[dict[str, Any]] = []
        for res in initial_results:
            key = (res.get("code", ""), res.get("name", ""))
            expected = expected_by_key.get(key)
            normalized, ok = _normalize_with_expected(res, expected)
            if ok or "error" in res:
                normalized_results.append(normalized)
                continue

            repaired = await _run_with_recover(
                payload.provider,
                lambda client, _exp=expected: _analyze_with_case_retry(
                    client=client,
                    prompt=_repair_prompt(_exp) if _exp else res.get("response", ""),
                    timeout_ms=payload.timeout_s * 1000,
                    retries=2,
                ),
            )
            normalized_results.append({**res, **repaired})
        return envelope(
            data={"provider": payload.provider, "count": len(normalized_results), "results": normalized_results}
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"{payload.provider} batch call failed: {exc}")


@router.get("/session/status", summary="All provider session status")
async def session_status_all():
    return envelope(data={p: _session_data(p) for p in _PROVIDER_CLIENTS})


@router.get("/session/status/{provider}", summary="Provider session status")
async def session_status(provider: Provider):
    return envelope(data=_session_data(provider))


@router.delete("/session/{provider}", summary="Close one provider session")
async def close_session(provider: Provider):
    await _close_provider_session(provider)
    return envelope(data={"provider": provider, "closed": True})


@router.delete("/session", summary="Close all provider sessions")
async def close_all_sessions():
    closed = []
    for provider, client_cls in _PROVIDER_CLIENTS.items():
        inst = getattr(client_cls, "_instance", None)
        if inst:
            await inst.close()
        closed.append(provider)
    return envelope(data={"closed": True, "providers": closed})
