#!/usr/bin/env python3
"""Channel health daemon for NewAPI gateway (v3 hybrid).

Uses NewAPI's built-in /api/channel/test/{id} for per-channel probing
(no upstream API keys required), with parallel execution and end-to-end
gateway-level content validation.

Designed to run as a systemd oneshot service behind a timer.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger("channel_health_daemon")

# ── Env helpers ────────────────────────────────────────────────────────────


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.environ.get(name, "")).strip()
    try:
        return max(minimum, int(raw))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = str(os.environ.get(name, "")).strip()
    try:
        return max(minimum, float(raw))
    except (TypeError, ValueError):
        return default


def _env_csv(name: str, default: list[str]) -> list[str]:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return list(default)
    values = [item.strip() for item in raw.replace("\n", ",").split(",")]
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result or list(default)


# ── Configuration ──────────────────────────────────────────────────────────

NEWAPI_BASE = os.environ.get("NEWAPI_BASE_URL", "http://localhost:3000").strip()
NEWAPI_USER = os.environ.get("NEWAPI_ADMIN_USER", "naadmin").strip()
NEWAPI_PASS = os.environ.get("NEWAPI_ADMIN_PASS", "").strip()

# Per-channel probe settings
PROBE_TIMEOUT = _env_float("CHANNEL_HEALTH_PROBE_TIMEOUT_SECONDS", 30.0, minimum=5.0)
PROBE_RETRIES = _env_int("CHANNEL_HEALTH_PROBE_RETRIES", 2, minimum=1)
PROBE_MAX_WORKERS = _env_int("CHANNEL_HEALTH_PROBE_MAX_WORKERS", 6, minimum=1)

# End-to-end gateway validation settings
GATEWAY_PROBE_TOKEN = os.environ.get(
    "CHANNEL_HEALTH_GATEWAY_TOKEN",
    "wpG93VkYDw5hlK2nedXpEkQQtDAM8YohRm3Q10RO37x76JYR",
).strip()
GATEWAY_PROBE_MODELS = _env_csv(
    "CHANNEL_HEALTH_GATEWAY_MODELS", ["gpt-5.4", "gpt-5.3-codex"]
)
GATEWAY_EXPECTED_TEXT = os.environ.get(
    "CHANNEL_HEALTH_EXPECTED_TEXT", "LIVE_OK"
).strip() or "LIVE_OK"
GATEWAY_PROBE_TIMEOUT = _env_float(
    "CHANNEL_HEALTH_GATEWAY_TIMEOUT_SECONDS", 45.0, minimum=10.0
)
GATEWAY_PROBE_RETRIES = _env_int("CHANNEL_HEALTH_GATEWAY_RETRIES", 2, minimum=1)

# Channels with weight <= this threshold are not auto-recovered
UNSTABLE_WEIGHT_THRESHOLD = _env_int(
    "CHANNEL_HEALTH_UNSTABLE_WEIGHT_THRESHOLD", 5, minimum=0
)

# Channel IDs to never mutate automatically
PROTECTED_CHANNEL_IDS = {
    int(item)
    for item in _env_csv("CHANNEL_HEALTH_PROTECTED_CHANNEL_IDS", [])
    if str(item).isdigit()
}

# Hard-blocking error patterns — immediate quarantine
HARD_BLOCK_PATTERNS = [
    "invalid_api_key",
    "unauthorized",
    "forbidden",
    "auth_unavailable",
    "token expired",
    "token invalidated",
    "model_not_found",
    "account_deactivated",
    "insufficient_quota",
]

# Soft-blocking patterns — transient, tolerate on single occurrence
SOFT_BLOCK_PATTERNS = [
    "429",
    "too many requests",
    "rate_limit",
    "503",
    "service_unavailable",
    "overloaded",
    "system_cpu_overloaded",
]


# ── Admin API helpers ──────────────────────────────────────────────────────


def admin_login(client: httpx.Client) -> None:
    resp = client.post(
        f"{NEWAPI_BASE}/api/user/login",
        json={"username": NEWAPI_USER, "password": NEWAPI_PASS},
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Admin login failed: {data.get('message', 'unknown')}")
    logger.info("Admin login OK (user: %s)", data.get("data", {}).get("username", "?"))


def admin_headers() -> dict[str, str]:
    return {"New-Api-User": "1", "Content-Type": "application/json"}


def list_channels(client: httpx.Client) -> list[dict[str, Any]]:
    channels: list[dict[str, Any]] = []
    page = 0
    page_size = 100
    while True:
        resp = client.get(
            f"{NEWAPI_BASE}/api/channel/",
            params={"p": page, "page_size": page_size},
            headers=admin_headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        inner = data.get("data", {})
        if isinstance(inner, dict):
            batch = inner.get("items") or []
        elif isinstance(inner, list):
            batch = inner
        else:
            break
        if not batch:
            break
        channels.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return channels


def update_channel_status(
    client: httpx.Client, channel: dict[str, Any], new_status: int
) -> bool:
    payload = {
        "id": channel["id"],
        "status": new_status,
        "name": channel.get("name", ""),
        "type": channel.get("type", 1),
        "key": channel.get("key", ""),
        "base_url": channel.get("base_url", ""),
        "models": channel.get("models", ""),
        "model_mapping": channel.get("model_mapping", ""),
        "groups": channel.get("group", channel.get("groups", "default")),
        "weight": channel.get("weight", 0),
        "priority": channel.get("priority", 0),
    }
    resp = client.put(
        f"{NEWAPI_BASE}/api/channel/",
        json=payload,
        headers=admin_headers(),
    )
    if resp.status_code == 200:
        try:
            body = resp.json()
        except ValueError:
            body = {}
        if body.get("success"):
            return True
    logger.warning(
        "Failed to update channel %d status to %d: %s",
        channel["id"],
        new_status,
        resp.text[:300],
    )
    return False


# ── Per-channel probing (via NewAPI built-in test) ─────────────────────────


def _find_pattern(text: str, patterns: list[str]) -> str | None:
    haystack = str(text or "").lower()
    for pattern in patterns:
        if pattern in haystack:
            return pattern
    return None


def probe_channel(
    admin_client: httpx.Client, channel: dict[str, Any]
) -> tuple[bool, str, float]:
    """Probe channel health using NewAPI's built-in test endpoint.

    GET /api/channel/test/{id} uses the gateway's stored keys internally.
    Returns (healthy, detail, latency_ms).
    """
    ch_id = channel.get("id", 0)
    for attempt in range(PROBE_RETRIES):
        start = time.monotonic()
        try:
            resp = admin_client.get(
                f"{NEWAPI_BASE}/api/channel/test/{ch_id}",
                headers=admin_headers(),
                timeout=PROBE_TIMEOUT,
            )
            latency = (time.monotonic() - start) * 1000

            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    api_time = data.get("time", 0)
                    return True, f"ok(api_time={api_time:.1f}s)", latency

                # Test failed — check message for patterns
                msg = str(data.get("message", "")).lower()
                pattern = _find_pattern(msg, HARD_BLOCK_PATTERNS)
                if pattern:
                    return False, f"hard_block:{pattern}", latency

                # Check for soft-block patterns (retry-worthy)
                soft = _find_pattern(msg, SOFT_BLOCK_PATTERNS)
                if soft and attempt < PROBE_RETRIES - 1:
                    time.sleep(2**attempt)
                    continue

                return (
                    False,
                    f"test_failed:{data.get('message', '?')[:200]}",
                    latency,
                )

            # Non-200. Retry if transient.
            if attempt < PROBE_RETRIES - 1:
                time.sleep(2**attempt)
                continue
            return False, f"http_{resp.status_code}", latency

        except httpx.TimeoutException:
            latency = (time.monotonic() - start) * 1000
            if attempt < PROBE_RETRIES - 1:
                time.sleep(2**attempt)
                continue
            return False, "timeout", latency

        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            if attempt < PROBE_RETRIES - 1:
                time.sleep(1)
                continue
            return False, f"error:{exc}", latency

    return False, "exhausted_retries", 0.0


# ── End-to-end gateway validation ─────────────────────────────────────────


def gateway_e2e_probe(model: str) -> tuple[bool, str, float]:
    """Send a real model request through the gateway and validate response content.

    This catches scenarios where individual channels report healthy but the
    actual model response is garbage/wrong/from a different model.
    Returns (passed, detail, latency_ms).
    """
    if not GATEWAY_PROBE_TOKEN:
        return False, "no_gateway_token", 0.0

    prompt = f"Reply with exactly: {GATEWAY_EXPECTED_TEXT}"
    for attempt in range(GATEWAY_PROBE_RETRIES):
        start = time.monotonic()
        try:
            with httpx.Client(
                timeout=GATEWAY_PROBE_TIMEOUT,
                verify=False,
                trust_env=False,
            ) as client:
                resp = client.post(
                    f"{NEWAPI_BASE}/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {GATEWAY_PROBE_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 16,
                    },
                )
            latency = (time.monotonic() - start) * 1000

            if resp.status_code != 200:
                snippet = resp.text[:300]
                pattern = _find_pattern(snippet, HARD_BLOCK_PATTERNS)
                if pattern:
                    return False, f"gateway_hard_block:{pattern}", latency
                if attempt < GATEWAY_PROBE_RETRIES - 1:
                    time.sleep(2**attempt)
                    continue
                return False, f"gateway_http_{resp.status_code}:{snippet[:100]}", latency

            try:
                body = resp.json()
            except ValueError:
                if attempt < GATEWAY_PROBE_RETRIES - 1:
                    time.sleep(2**attempt)
                    continue
                return False, "gateway_invalid_json", latency

            # Extract response content
            content = ""
            choices = body.get("choices") or []
            if choices:
                msg = choices[0].get("message") or choices[0].get("delta") or {}
                content = str(msg.get("content", "")).strip()

            # Validate content
            if GATEWAY_EXPECTED_TEXT in content:
                return True, f"gateway_ok(model={model}, content={content!r})", latency

            # Content mismatch — model returned something unexpected
            if attempt < GATEWAY_PROBE_RETRIES - 1:
                time.sleep(2**attempt)
                continue
            return (
                False,
                f"gateway_content_mismatch(model={model}, got={content[:100]!r})",
                latency,
            )

        except httpx.TimeoutException:
            latency = (time.monotonic() - start) * 1000
            if attempt < GATEWAY_PROBE_RETRIES - 1:
                time.sleep(2**attempt)
                continue
            return False, f"gateway_timeout(model={model})", latency

        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            if attempt < GATEWAY_PROBE_RETRIES - 1:
                time.sleep(1)
                continue
            return False, f"gateway_error:{exc}", latency

    return False, "gateway_exhausted_retries", 0.0


# ── Main logic ─────────────────────────────────────────────────────────────


def run_health_check(
    dry_run: bool = False, log_dir: str | None = None
) -> dict[str, Any]:
    audit: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "probe_max_workers": PROBE_MAX_WORKERS,
        "gateway_models": list(GATEWAY_PROBE_MODELS),
        "gateway_expected_text": GATEWAY_EXPECTED_TEXT,
        "channels_probed": 0,
        "channels_enabled": 0,
        "channels_disabled": 0,
        "channels_recovered": 0,
        "channels_healthy": 0,
        "channels_unhealthy": 0,
        "gateway_e2e": [],
        "details": [],
    }

    with httpx.Client(timeout=60.0, verify=False, trust_env=False) as admin_client:
        admin_login(admin_client)
        channels = list_channels(admin_client)
        logger.info("Fetched %d channels from NewAPI", len(channels))

        # Filter probe targets
        probe_targets: list[dict[str, Any]] = []
        for channel in channels:
            channel_id = int(channel.get("id", 0) or 0)
            status = int(channel.get("status", 0) or 0)
            if channel_id in PROTECTED_CHANNEL_IDS:
                continue
            if status not in (1, 2):
                continue
            probe_targets.append(channel)

        logger.info(
            "Probing %d channels with up to %d parallel workers",
            len(probe_targets),
            PROBE_MAX_WORKERS,
        )

        # ── Phase 1: Parallel per-channel probing ──────────────────────
        probe_results: dict[int, tuple[bool, str, float]] = {}
        if probe_targets:
            max_workers = min(PROBE_MAX_WORKERS, len(probe_targets))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_channel = {
                    executor.submit(probe_channel, admin_client, ch): ch
                    for ch in probe_targets
                }
                for future in as_completed(future_to_channel):
                    ch = future_to_channel[future]
                    ch_id = int(ch.get("id", 0) or 0)
                    try:
                        probe_results[ch_id] = future.result()
                    except Exception as exc:
                        probe_results[ch_id] = (False, f"worker_error:{exc}", 0.0)

        # ── Phase 2: Apply status changes ──────────────────────────────
        for channel in probe_targets:
            ch_id = int(channel.get("id", 0) or 0)
            ch_name = str(channel.get("name") or "?")
            ch_status = int(channel.get("status", 0) or 0)
            ch_weight = int(channel.get("weight", 0) or 0)
            healthy, detail, latency = probe_results.get(
                ch_id, (False, "probe_missing", 0.0)
            )

            audit["channels_probed"] += 1
            action = "none"

            if healthy:
                audit["channels_healthy"] += 1
                if ch_status == 2:
                    # Recovery candidate
                    if ch_weight > UNSTABLE_WEIGHT_THRESHOLD:
                        action = "recover"
                        if not dry_run:
                            if update_channel_status(admin_client, channel, 1):
                                audit["channels_recovered"] += 1
                                logger.info(
                                    "RECOVERED channel %d (%s)", ch_id, ch_name
                                )
                            else:
                                action = "recover_failed"
                    else:
                        action = "skip_unstable"
            else:
                audit["channels_unhealthy"] += 1
                if ch_status == 1:
                    # Quarantine: disable the unhealthy channel
                    action = "quarantine"
                    if not dry_run:
                        if update_channel_status(admin_client, channel, 2):
                            audit["channels_disabled"] += 1
                            logger.warning(
                                "QUARANTINED channel %d (%s) — %s",
                                ch_id,
                                ch_name,
                                detail,
                            )
                        else:
                            action = "quarantine_failed"

            audit["details"].append(
                {
                    "channel_id": ch_id,
                    "name": ch_name,
                    "status_before": ch_status,
                    "weight": ch_weight,
                    "healthy": healthy,
                    "detail": detail,
                    "latency_ms": round(latency, 1),
                    "action": action,
                }
            )

    # ── Phase 3: End-to-end gateway validation ─────────────────────────
    logger.info(
        "Running end-to-end gateway validation for models: %s",
        ", ".join(GATEWAY_PROBE_MODELS),
    )
    for model in GATEWAY_PROBE_MODELS:
        passed, detail, latency = gateway_e2e_probe(model)
        status_str = "PASS" if passed else "FAIL"
        logger.info(
            "  Gateway E2E [%s] %s: %s (%.0fms)", model, status_str, detail, latency
        )
        audit["gateway_e2e"].append(
            {
                "model": model,
                "passed": passed,
                "detail": detail,
                "latency_ms": round(latency, 1),
            }
        )

    # Write audit log
    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        audit_file = log_path / f"health_check_{timestamp}.json"
        audit_file.write_text(
            json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # Keep only last 1000 audit files
        all_audits = sorted(log_path.glob("health_check_*.json"))
        for old_file in all_audits[:-1000]:
            old_file.unlink(missing_ok=True)

    return audit


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NewAPI Channel Health Daemon (v3 hybrid)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Probe only; do not mutate channel status.",
    )
    parser.add_argument(
        "--log-dir",
        default="/var/log/newapi-channel-health",
        help="Directory for JSON audit logs.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not NEWAPI_PASS:
        logger.error("NEWAPI_ADMIN_PASS environment variable is required")
        sys.exit(1)

    try:
        audit = run_health_check(dry_run=args.dry_run, log_dir=args.log_dir)
    except Exception:
        logger.exception("Health check failed")
        sys.exit(1)

    # Summary
    logger.info(
        "Health check complete: probed=%d healthy=%d unhealthy=%d "
        "recovered=%d quarantined=%d",
        audit["channels_probed"],
        audit["channels_healthy"],
        audit["channels_unhealthy"],
        audit["channels_recovered"],
        audit["channels_disabled"],
    )
    for e2e in audit["gateway_e2e"]:
        logger.info(
            "  Gateway E2E %s: %s (%s, %.0fms)",
            e2e["model"],
            "PASS" if e2e["passed"] else "FAIL",
            e2e["detail"],
            e2e["latency_ms"],
        )
    if audit["channels_disabled"] or audit["channels_recovered"]:
        for detail in audit["details"]:
            if detail["action"] not in ("none", "skip_unstable"):
                print(
                    f"  [{detail['action'].upper()}] #{detail['channel_id']} "
                    f"{detail['name']} — {detail['detail']} ({detail['latency_ms']:.0f}ms)"
                )


if __name__ == "__main__":
    main()
