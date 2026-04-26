"""routes_governance.py - governance catalog APIs (admin scope)."""
from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.governance.build_feature_catalog import (
    summarize_catalog_audit_scope,
    summarize_catalog_denominators,
    summarize_catalog_features,
    summarize_feature_traceability,
)
from app.core.response import envelope
from app.core.security import get_current_user_optional
from app.services.governance_catalog_live import get_live_governance_catalog

features_router = APIRouter(prefix="/api/v1/features", tags=["governance"])
governance_router = APIRouter(prefix="/api/v1/governance", tags=["governance"])

SNAPSHOT_PATH = Path(__file__).resolve().parents[1] / "governance" / "catalog_snapshot.json"
_EMPTY_CATALOG_SUMMARY = summarize_catalog_features([])


async def _require_admin(request: Request):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if (user.role or "").lower() not in {"admin", "super_admin"}:
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    return user


def _normalize_catalog_rollups(payload: dict) -> dict:
    features = list(payload.get("features", []))
    rollup = summarize_catalog_features(features)
    payload.setdefault("total", len(features))
    payload["status_summary"] = payload.get("status_summary") or dict(rollup["status_summary"])
    payload["negative_status_summary"] = payload.get("negative_status_summary") or dict(rollup["negative_status_summary"])
    if "latest_feature_verified_at" not in payload:
        payload["latest_feature_verified_at"] = rollup["latest_feature_verified_at"]
    payload["denominator_summary"] = payload.get("denominator_summary") or summarize_catalog_denominators(features)
    payload["audit_scope_summary"] = payload.get("audit_scope_summary") or summarize_catalog_audit_scope(features)
    payload["feature_traceability_summary"] = payload.get("feature_traceability_summary") or summarize_feature_traceability(features)
    return payload


def _effective_latest_feature_verified_at(catalog: dict, filtered_rollup: dict, *, filtered: bool) -> str | None:
    if "latest_feature_verified_at" not in catalog:
        return filtered_rollup["latest_feature_verified_at"]

    catalog_latest = catalog.get("latest_feature_verified_at")
    freshness = str(catalog.get("test_result_freshness") or "").strip().lower()
    if catalog_latest is None and freshness != "fresh":
        return None
    if filtered:
        return filtered_rollup["latest_feature_verified_at"]
    return catalog_latest


def _load_snapshot_catalog() -> dict:
    if not SNAPSHOT_PATH.exists():
        return _normalize_catalog_rollups({
            "generated_at": None,
            "catalog_mode": "snapshot",
            "total": 0,
            "status_summary": dict(_EMPTY_CATALOG_SUMMARY["status_summary"]),
            "negative_status_summary": dict(_EMPTY_CATALOG_SUMMARY["negative_status_summary"]),
            "latest_feature_verified_at": _EMPTY_CATALOG_SUMMARY["latest_feature_verified_at"],
            "test_collection_summary": None,
            "features": [],
        })
    payload = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    payload["catalog_mode"] = "snapshot"
    return _normalize_catalog_rollups(payload)


def _get_catalog(source: str) -> dict:
    if source == "snapshot":
        return _load_snapshot_catalog()
    return get_live_governance_catalog()


def _filter_catalog(
    catalog: dict,
    *,
    fr_id: str | None,
    visibility: str | None,
    status: str | None,
    source: str,
) -> dict:
    features = list(catalog.get("features", []))
    if fr_id:
        features = [feature for feature in features if feature.get("fr_id") == fr_id]
    if visibility:
        features = [feature for feature in features if feature.get("visibility") == visibility]
    if status:
        features = [feature for feature in features if feature.get("structural_status") == status]

    filtered_rollup = summarize_catalog_features(features)
    denominator_summary = summarize_catalog_denominators(features)
    audit_scope_summary = summarize_catalog_audit_scope(features)
    feature_traceability_summary = summarize_feature_traceability(features)
    filtered_summary = dict(filtered_rollup["status_summary"])

    red_count = sum(
        1 for feature in features if (feature.get("structural_status") or "").startswith("BLOCKED")
    )
    ready_count = sum(1 for feature in features if feature.get("structural_status") == "READY")
    catalog_mode = "snapshot" if source == "snapshot" else str(catalog.get("catalog_mode") or "live")
    source_label = source
    filtered = any(value is not None for value in (fr_id, visibility, status))
    return {
        "source": source_label,
        "catalog_mode": catalog_mode,
        "total": len(features),
        "red_count": red_count,
        "ready_count": ready_count,
        "generated_at": catalog.get("generated_at"),
        "registry_generated_at": catalog.get("registry_generated_at"),
        "test_result_source": catalog.get("test_result_source"),
        "test_result_generated_at": catalog.get("test_result_generated_at"),
        "test_result_freshness": catalog.get("test_result_freshness"),
        "test_result_age_seconds": catalog.get("test_result_age_seconds"),
        "test_result_stale_reason": catalog.get("test_result_stale_reason"),
        "test_collection_summary": catalog.get("test_collection_summary"),
        "denominator_summary": denominator_summary,
        "audit_scope_summary": audit_scope_summary,
        "feature_traceability_summary": feature_traceability_summary,
        "catalog_total": denominator_summary["catalog_total"],
        "eligible_total": denominator_summary["eligible_total"],
        "negative_total": denominator_summary["negative_total"],
        "ready_strict_count": denominator_summary["ready_strict_count"],
        "ready_with_gaps_count": denominator_summary["ready_with_gaps_count"],
        "blocked_count": denominator_summary["blocked_count"],
        "fr_inference_only_count": feature_traceability_summary["fr_inference_fallback"],
        "latest_feature_verified_at": _effective_latest_feature_verified_at(
            catalog,
            filtered_rollup,
            filtered=filtered,
        ),
        "negative_status_summary": filtered_rollup["negative_status_summary"],
        "cache_ttl_seconds": catalog.get("cache_ttl_seconds"),
        "status_summary": filtered_summary,
        "features": features,
    }


async def _catalog_response(
    request: Request,
    fr_id: str | None,
    visibility: str | None,
    status: str | None,
    source: str,
    user,
):
    del request, user
    catalog = _get_catalog(source)
    return envelope(
        data=_filter_catalog(catalog, fr_id=fr_id, visibility=visibility, status=status, source=source)
    )


@features_router.get("/catalog")
async def features_catalog(
    request: Request,
    fr_id: str | None = Query(default=None, description="Filter by FR"),
    visibility: str | None = Query(default=None, description="Filter by visibility"),
    status: str | None = Query(default=None, description="Filter by structural status"),
    source: str = Query(default="live", pattern=r"^(live|snapshot)$"),
    user=Depends(_require_admin),
):
    return await _catalog_response(request, fr_id, visibility, status, source, user)


@governance_router.get("/catalog")
async def governance_catalog(
    request: Request,
    fr_id: str | None = Query(default=None, description="Filter by FR"),
    visibility: str | None = Query(default=None, description="Filter by visibility"),
    status: str | None = Query(default=None, description="Filter by structural status"),
    source: str = Query(default="live", pattern=r"^(live|snapshot)$"),
    user=Depends(_require_admin),
):
    return await _catalog_response(request, fr_id, visibility, status, source, user)
