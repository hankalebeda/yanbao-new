from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from threading import Lock

from app.governance.build_feature_catalog import (
    build_catalog_snapshot_payload,
    count_collected_test_nodes,
    load_junit_node_verified_at,
    load_latest_junit_results_bundle,
    load_registry,
    resolve_junit_source_path,
    scan_fastapi_routes,
    scan_html_templates,
    collect_pytest_nodes,
    split_test_collection_map,
)

_CACHE_TTL = timedelta(minutes=5)
_CACHE_LOCK = Lock()
_CACHE: dict[str, object] = {
    "built_at": None,
    "catalog": None,
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _latest_junit_results_bundle(
    *,
    expected_total: int | None = None,
) -> tuple[dict[str, str], str | None, str | None, str, int | None, str | None]:
    return load_latest_junit_results_bundle(expected_total=expected_total)


def _latest_junit_node_verified_at(junit_source: str | None) -> dict[str, str]:
    return load_junit_node_verified_at(resolve_junit_source_path(junit_source))


def _build_live_catalog() -> dict:
    registry = load_registry()
    code_routes = scan_fastapi_routes()
    html_templates = scan_html_templates()
    test_nodes_raw = collect_pytest_nodes()
    test_nodes_by_feature, test_collection_summary = split_test_collection_map(test_nodes_raw)
    expected_total = count_collected_test_nodes(test_nodes_by_feature, test_collection_summary) or None
    junit_bundle = _latest_junit_results_bundle(expected_total=expected_total)
    node_verified_at = _latest_junit_node_verified_at(junit_bundle[1])
    generated_at = _now_utc().isoformat()
    snapshot, _ = build_catalog_snapshot_payload(
        registry=registry,
        code_routes=code_routes,
        html_templates=html_templates,
        test_nodes_raw=test_nodes_raw,
        generated_at=generated_at,
        junit_bundle=junit_bundle,
        node_verified_at=node_verified_at,
    )

    return {
        "catalog_mode": "live",
        "cache_ttl_seconds": int(_CACHE_TTL.total_seconds()),
        **snapshot,
    }


def get_live_governance_catalog(*, force_refresh: bool = False) -> dict:
    now = _now_utc()
    with _CACHE_LOCK:
        built_at = _CACHE.get("built_at")
        cached = _CACHE.get("catalog")
        if (
            not force_refresh
            and isinstance(built_at, datetime)
            and isinstance(cached, dict)
            and now - built_at <= _CACHE_TTL
        ):
            return deepcopy(cached)

        catalog = _build_live_catalog()
        _CACHE["built_at"] = now
        _CACHE["catalog"] = catalog
        return deepcopy(catalog)
