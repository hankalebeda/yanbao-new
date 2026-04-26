from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

try:
    import paramiko
except ModuleNotFoundError:  # pragma: no cover
    paramiko = None  # type: ignore[assignment]


CHANNEL_ID_PATTERN = re.compile(r'"channel_id":\s*(\d+)')
DEFAULT_PROMPT = "Reply with exactly LIVE_OK"


@dataclass
class ChannelSnapshot:
    channel_id: int
    name: str
    used_quota: int
    response_time: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run multi-round requests through New API and verify channel coverage."
    )
    parser.add_argument("--base-url", required=True, help="New API base URL, e.g. http://host:3000")
    parser.add_argument("--api-key", required=True, help="Aggregated token for /v1/responses")
    parser.add_argument("--admin-username", required=True, help="New API admin username")
    parser.add_argument("--admin-password", required=True, help="New API admin password")
    parser.add_argument("--model", default="gpt-5.4", help="Model to request")
    parser.add_argument("--max-requests", type=int, default=80, help="Maximum verification requests")
    parser.add_argument("--refresh-every", type=int, default=5, help="Refresh channel stats every N requests")
    parser.add_argument("--timeout", type=float, default=90.0, help="Per-request timeout")
    parser.add_argument("--ssh-host", default=None, help="Optional SSH host for docker log parsing")
    parser.add_argument("--ssh-user", default=None, help="Optional SSH user")
    parser.add_argument("--ssh-password", default=None, help="Optional SSH password")
    parser.add_argument("--ssh-container", default="new-api", help="Docker container name for log parsing")
    parser.add_argument("--out", default=None, help="Optional JSON output path")
    return parser.parse_args()


class NewAPIAdmin:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=30.0,
            follow_redirects=True,
            trust_env=False,
        )
        self.username = username
        self.password = password
        self.user_id: int | None = None

    def close(self) -> None:
        self.client.close()

    def login(self) -> None:
        response = self.client.post("/api/user/login", json={"username": self.username, "password": self.password})
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or {}
        user_id = data.get("id")
        if not isinstance(user_id, int):
            raise RuntimeError(f"login response missing user id: {payload}")
        self.user_id = user_id

    def _headers(self) -> dict[str, str]:
        if self.user_id is None:
            raise RuntimeError("admin client not logged in")
        return {"New-Api-User": str(self.user_id)}

    def fetch_channels(self) -> dict[int, ChannelSnapshot]:
        response = self.client.get("/api/channel/?p=1&page_size=200", headers=self._headers())
        response.raise_for_status()
        payload = response.json()
        items = ((payload.get("data") or {}).get("items") or [])
        result: dict[int, ChannelSnapshot] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            channel_id = item.get("id")
            if not isinstance(channel_id, int):
                continue
            result[channel_id] = ChannelSnapshot(
                channel_id=channel_id,
                name=str(item.get("name") or channel_id),
                used_quota=int(item.get("used_quota") or 0),
                response_time=int(item["response_time"]) if item.get("response_time") is not None else None,
            )
        return result


def _extract_channel_hits_from_logs(log_text: str) -> dict[int, int]:
    counts: dict[int, int] = {}
    for match in CHANNEL_ID_PATTERN.finditer(log_text):
        channel_id = int(match.group(1))
        counts[channel_id] = counts.get(channel_id, 0) + 1
    return counts


def _fetch_logs_via_ssh(host: str, user: str, password: str, container: str, since_utc: datetime) -> str:
    if paramiko is None:
        raise RuntimeError("paramiko is not installed")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=user, password=password, timeout=15)
    try:
        since_value = since_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        command = f"docker logs --since '{since_value}' {container} 2>&1"
        stdin, stdout, stderr = client.exec_command(command, timeout=120)
        output = stdout.read().decode("utf-8", "replace")
        error = stderr.read().decode("utf-8", "replace")
        return output + error
    finally:
        client.close()


def _call_responses(client: httpx.Client, base_url: str, api_key: str, model: str, timeout: float) -> dict[str, Any]:
    payload = {
        "model": model,
        "input": DEFAULT_PROMPT,
        "max_output_tokens": 16,
        "store": False,
        "reasoning": {"effort": "xhigh"},
    }
    started = time.perf_counter()
    try:
        response = client.post(
            base_url.rstrip("/") + "/v1/responses",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=timeout,
        )
        elapsed = round(time.perf_counter() - started, 3)
        ok = response.status_code == 200 and "LIVE_OK" in response.text
        return {
            "status_code": response.status_code,
            "elapsed_s": elapsed,
            "ok": ok,
            "body": response.text[:500],
        }
    except Exception as exc:  # pragma: no cover - operational network script
        elapsed = round(time.perf_counter() - started, 3)
        return {
            "status_code": None,
            "elapsed_s": elapsed,
            "ok": False,
            "body": f"{type(exc).__name__}: {exc}",
        }


def main() -> int:
    args = parse_args()
    admin = NewAPIAdmin(args.base_url, args.admin_username, args.admin_password)
    admin.login()
    before_channels = admin.fetch_channels()
    started_at = datetime.now(timezone.utc)

    active_ids = sorted(before_channels.keys())
    request_results: list[dict[str, Any]] = []
    covered_ids: set[int] = set()

    with httpx.Client(follow_redirects=True, trust_env=False) as client:
        for index in range(1, args.max_requests + 1):
            result = _call_responses(client, args.base_url, args.api_key, args.model, args.timeout)
            result["request_index"] = index
            request_results.append(result)

            if index % max(args.refresh_every, 1) == 0 or index == args.max_requests:
                current_channels = admin.fetch_channels()
                for channel_id, snapshot in current_channels.items():
                    previous = before_channels.get(channel_id)
                    if previous is None:
                        continue
                    if snapshot.used_quota > previous.used_quota:
                        covered_ids.add(channel_id)
                if covered_ids.issuperset(active_ids):
                    break

    after_channels = admin.fetch_channels()
    admin.close()

    quota_deltas: list[dict[str, Any]] = []
    for channel_id in sorted(after_channels):
        current = after_channels[channel_id]
        previous = before_channels.get(channel_id)
        if previous is None:
            continue
        delta = current.used_quota - previous.used_quota
        quota_deltas.append(
            {
                "channel_id": channel_id,
                "name": current.name,
                "used_quota_before": previous.used_quota,
                "used_quota_after": current.used_quota,
                "used_quota_delta": delta,
                "response_time_ms": current.response_time,
                "covered_by_quota_delta": delta > 0,
            }
        )

    logs_text = ""
    log_hit_counts: dict[int, int] = {}
    if args.ssh_host and args.ssh_user and args.ssh_password:
        logs_text = _fetch_logs_via_ssh(
            args.ssh_host,
            args.ssh_user,
            args.ssh_password,
            args.ssh_container,
            started_at,
        )
        log_hit_counts = _extract_channel_hits_from_logs(logs_text)

    coverage_by_delta = {item["channel_id"] for item in quota_deltas if item["covered_by_quota_delta"]}
    coverage_by_logs = {channel_id for channel_id, count in log_hit_counts.items() if count > 0}
    covered_union = coverage_by_delta | coverage_by_logs

    summary = {
        "base_url": args.base_url.rstrip("/"),
        "model": args.model,
        "max_requests": args.max_requests,
        "requests_sent": len(request_results),
        "success_count": sum(1 for item in request_results if item["ok"]),
        "failure_count": sum(1 for item in request_results if not item["ok"]),
        "request_results": request_results,
        "active_channel_ids": active_ids,
        "active_channel_names": {item["channel_id"]: item["name"] for item in quota_deltas},
        "quota_deltas": quota_deltas,
        "log_hit_counts": log_hit_counts,
        "coverage_by_quota_delta": sorted(coverage_by_delta),
        "coverage_by_logs": sorted(coverage_by_logs),
        "covered_union": sorted(covered_union),
        "missing_channel_ids": sorted(set(active_ids) - covered_union),
        "all_channels_covered": covered_union.issuperset(active_ids),
    }

    if args.out:
        out_path = Path(args.out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["all_channels_covered"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
