#!/usr/bin/env python3
"""Executable scaffold for the live browser + deep-fix loop.

This script does not pretend to discover or fix issues automatically.
Instead, it provides machine-readable loop bookkeeping plus a hard
precheck that verifies the live site and a real browser are both usable.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

try:
    from scripts.github_automation_paths import (
        legacy_live_fix_loop_dir,
        live_fix_loop_dir as github_live_fix_loop_dir,
        seed_dir_from_legacy,
    )
except ModuleNotFoundError:
    from github_automation_paths import (
        legacy_live_fix_loop_dir,
        live_fix_loop_dir as github_live_fix_loop_dir,
        seed_dir_from_legacy,
    )


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_PAGE_PATHS = ["/", "/login", "/reports"]
RECENT_HISTORY_LIMIT = 5


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ledger_dir(root: Path) -> Path:
    target = github_live_fix_loop_dir(root)
    seed_dir_from_legacy(target, legacy_live_fix_loop_dir(root))
    return target


def issue_register_path(root: Path) -> Path:
    return ledger_dir(root) / "issue_register.md"


def review_log_path(root: Path) -> Path:
    return ledger_dir(root) / "review_log.md"


def issue_register_template() -> str:
    return """# Live Fix Loop Issue Register

- current_round_id: bootstrap
- open_issue_count: 0
- closed_issue_count: 0
- zero_open_streak: 0
- recent_open_issue_counts: [0]
- last_focus: bootstrap
- last_summary: bootstrap initialized; run precheck before the first live repair round
- last_next_focus: precheck
- stop_allowed: false

## Round Summaries

- bootstrap / focus=bootstrap / new_high_value_issue_count=0 / fixed_count=0 / reopened_issue_count=0 / regression_failures=0 / open_issue_count=0 / closed_issue_count=0 / next_focus=precheck / summary=live fix loop ledger initialized

## Open Issues

| issue_id | round | severity | scope | ssot_refs | runtime_evidence | root_cause | fix_status |
| --- | --- | --- | --- | --- | --- | --- | --- |
(none)

## Closed Issues

| issue_id | round | severity | scope | ssot_refs | runtime_evidence | root_cause | fix_status | verification |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
(none)
"""


def review_log_template() -> str:
    return """# Live Fix Loop Review Log

- current_round_id: bootstrap
- rounds_recorded: 0
- consecutive_clean_rounds: 0
- stop_allowed: false
- last_result: bootstrap initialized; run precheck before the first live repair round

## Review Entries

- bootstrap: live fix loop files created
"""


def ensure_ledgers(root: Path, *, force: bool = False) -> dict[str, Path]:
    target_dir = ledger_dir(root)
    target_dir.mkdir(parents=True, exist_ok=True)

    issue_path = issue_register_path(root)
    review_path = review_log_path(root)

    if force or not issue_path.exists():
        issue_path.write_text(issue_register_template(), encoding="utf-8")
    if force or not review_path.exists():
        review_path.write_text(review_log_template(), encoding="utf-8")

    return {"issue_register": issue_path, "review_log": review_path}


def require_ledgers(root: Path) -> dict[str, Path]:
    issue_path = issue_register_path(root)
    review_path = review_log_path(root)
    if not issue_path.exists() or not review_path.exists():
        raise FileNotFoundError(
            "live fix loop ledgers are missing; run `python scripts/live_fix_loop.py init` first"
        )
    return {"issue_register": issue_path, "review_log": review_path}


def _parse_metadata(text: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in text.splitlines():
        match = re.match(r"^- ([a-zA-Z0-9_]+):\s*(.*)$", line)
        if not match:
            if line.startswith("## "):
                break
            continue
        metadata[match.group(1)] = match.group(2).strip()
    return metadata


def _replace_metadata_value(text: str, key: str, value: str) -> str:
    pattern = rf"(?m)^- {re.escape(key)}:\s*.*$"
    replacement = f"- {key}: {value}"
    if re.search(pattern, text):
        return re.sub(pattern, replacement, text, count=1)
    marker = "## "
    idx = text.find(marker)
    if idx == -1:
        return text.rstrip() + "\n" + replacement + "\n"
    return text[:idx] + replacement + "\n\n" + text[idx:]


def _insert_bullet_under_heading(text: str, heading: str, bullet: str, *, before_heading: str | None = None) -> str:
    anchor = f"## {heading}"
    start = text.find(anchor)
    if start == -1:
        raise ValueError(f"heading not found: {heading}")
    insert_at = len(text)
    if before_heading:
        before_anchor = f"## {before_heading}"
        next_idx = text.find(before_anchor, start + len(anchor))
        if next_idx != -1:
            insert_at = next_idx
    segment = text[start:insert_at]
    if not segment.endswith("\n\n"):
        if not segment.endswith("\n"):
            segment += "\n"
        segment += "\n"
    segment += bullet.rstrip() + "\n"
    return text[:start] + segment + text[insert_at:]


def _parse_int(metadata: dict[str, str], key: str, default: int = 0) -> int:
    raw = metadata.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_bool(metadata: dict[str, str], key: str, default: bool = False) -> bool:
    raw = metadata.get(key)
    if raw is None:
        return default
    return raw.strip().lower() == "true"


def _parse_recent_counts(metadata: dict[str, str]) -> list[int]:
    raw = metadata.get("recent_open_issue_counts", "[0]")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return [0]
    if not isinstance(parsed, list):
        return [0]
    result: list[int] = []
    for item in parsed:
        if isinstance(item, int):
            result.append(item)
    return result or [0]


def _read_state(root: Path) -> dict[str, Any]:
    paths = require_ledgers(root)
    issue_text = paths["issue_register"].read_text(encoding="utf-8")
    review_text = paths["review_log"].read_text(encoding="utf-8")
    issue_meta = _parse_metadata(issue_text)
    review_meta = _parse_metadata(review_text)
    return {
        "paths": paths,
        "issue_text": issue_text,
        "review_text": review_text,
        "issue_meta": issue_meta,
        "review_meta": review_meta,
        "current_round_id": issue_meta.get("current_round_id", review_meta.get("current_round_id", "bootstrap")),
        "open_issue_count": _parse_int(issue_meta, "open_issue_count"),
        "closed_issue_count": _parse_int(issue_meta, "closed_issue_count"),
        "zero_open_streak": _parse_int(issue_meta, "zero_open_streak"),
        "recent_open_issue_counts": _parse_recent_counts(issue_meta),
        "last_focus": issue_meta.get("last_focus", "bootstrap"),
        "last_summary": issue_meta.get("last_summary", ""),
        "last_next_focus": issue_meta.get("last_next_focus", ""),
        "stop_allowed": _parse_bool(issue_meta, "stop_allowed"),
        "rounds_recorded": _parse_int(review_meta, "rounds_recorded"),
        "consecutive_clean_rounds": _parse_int(review_meta, "consecutive_clean_rounds"),
        "last_result": review_meta.get("last_result", ""),
    }


def _is_clean_round(
    *,
    open_issue_count: int,
    new_high_value_issue_count: int,
    reopened_issue_count: int,
    regression_failures: int,
) -> bool:
    return (
        open_issue_count == 0
        and new_high_value_issue_count == 0
        and reopened_issue_count == 0
        and regression_failures == 0
    )


def append_round(
    root: Path,
    *,
    round_id: str,
    focus: str,
    new_high_value_issue_count: int,
    fixed_count: int,
    reopened_issue_count: int,
    regression_failures: int,
    open_issue_count: int,
    closed_issue_count: int,
    next_focus: str,
    summary: str,
) -> dict[str, Any]:
    if any(
        count < 0
        for count in (
            new_high_value_issue_count,
            fixed_count,
            reopened_issue_count,
            regression_failures,
            open_issue_count,
            closed_issue_count,
        )
    ):
        raise ValueError("round metrics must be non-negative integers")

    state = _read_state(root)
    clean_round = _is_clean_round(
        open_issue_count=open_issue_count,
        new_high_value_issue_count=new_high_value_issue_count,
        reopened_issue_count=reopened_issue_count,
        regression_failures=regression_failures,
    )
    zero_open_streak = state["zero_open_streak"] + 1 if clean_round else 0
    stop_allowed = zero_open_streak >= 2
    recent_counts = [open_issue_count, *state["recent_open_issue_counts"]][:RECENT_HISTORY_LIMIT]

    issue_summary_line = (
        f"- {round_id} / focus={focus} / new_high_value_issue_count={new_high_value_issue_count} "
        f"/ fixed_count={fixed_count} / reopened_issue_count={reopened_issue_count} "
        f"/ regression_failures={regression_failures} / open_issue_count={open_issue_count} "
        f"/ closed_issue_count={closed_issue_count} / next_focus={next_focus} / summary={summary}"
    )
    review_summary_line = (
        f"- {round_id} / focus={focus} / new_high_value_issue_count={new_high_value_issue_count} "
        f"/ fixed_count={fixed_count} / reopened_issue_count={reopened_issue_count} "
        f"/ regression_failures={regression_failures} / open_issue_count={open_issue_count} "
        f"/ closed_issue_count={closed_issue_count} / stop_allowed={str(stop_allowed).lower()} "
        f"/ next_focus={next_focus} / summary={summary}"
    )

    issue_text = state["issue_text"]
    for key, value in (
        ("current_round_id", round_id),
        ("open_issue_count", str(open_issue_count)),
        ("closed_issue_count", str(closed_issue_count)),
        ("zero_open_streak", str(zero_open_streak)),
        ("recent_open_issue_counts", json.dumps(recent_counts, ensure_ascii=False)),
        ("last_focus", focus),
        ("last_summary", summary),
        ("last_next_focus", next_focus),
        ("stop_allowed", str(stop_allowed).lower()),
    ):
        issue_text = _replace_metadata_value(issue_text, key, value)
    issue_text = _insert_bullet_under_heading(
        issue_text,
        "Round Summaries",
        issue_summary_line,
        before_heading="Open Issues",
    )

    review_text = state["review_text"]
    for key, value in (
        ("current_round_id", round_id),
        ("rounds_recorded", str(state["rounds_recorded"] + 1)),
        ("consecutive_clean_rounds", str(zero_open_streak)),
        ("stop_allowed", str(stop_allowed).lower()),
        ("last_result", summary),
    ):
        review_text = _replace_metadata_value(review_text, key, value)
    review_text = _insert_bullet_under_heading(review_text, "Review Entries", review_summary_line)

    state["paths"]["issue_register"].write_text(issue_text, encoding="utf-8")
    state["paths"]["review_log"].write_text(review_text, encoding="utf-8")

    return {
        "round_id": round_id,
        "focus": focus,
        "new_high_value_issue_count": new_high_value_issue_count,
        "fixed_count": fixed_count,
        "reopened_issue_count": reopened_issue_count,
        "regression_failures": regression_failures,
        "open_issue_count": open_issue_count,
        "closed_issue_count": closed_issue_count,
        "zero_open_streak": zero_open_streak,
        "stop_allowed": stop_allowed,
        "next_focus": next_focus,
        "summary": summary,
    }


def _health_probe(base_url: str, timeout: int) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/health"
    request = urllib.request.Request(url, headers={"User-Agent": "live-fix-loop/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            status_code = getattr(response, "status", 200)
    except urllib.error.HTTPError as exc:
        return {
            "ok": False,
            "url": url,
            "status_code": exc.code,
            "reason": f"http_error:{exc.code}",
        }
    except Exception as exc:  # pragma: no cover - network/OS specific
        return {
            "ok": False,
            "url": url,
            "status_code": None,
            "reason": f"request_failed:{exc}",
        }

    parsed: Any = None
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = body

    ok = False
    if isinstance(parsed, dict):
        if parsed.get("success") is True:
            ok = True
        elif parsed.get("status") == "ok":
            ok = True
        elif isinstance(parsed.get("data"), dict) and parsed["data"].get("status") == "ok":
            ok = True

    return {
        "ok": ok and status_code == 200,
        "url": url,
        "status_code": status_code,
        "body": parsed,
    }


def _browser_probe(base_url: str, page_paths: list[str], timeout_ms: int) -> dict[str, Any]:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover - depends on local install
        return {
            "ok": False,
            "reason": f"playwright_unavailable:{exc}",
            "checked_paths": page_paths,
        }

    checked_pages: list[dict[str, Any]] = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            for path in page_paths:
                target = f"{base_url.rstrip('/')}{path}"
                try:
                    response = page.goto(target, wait_until="load", timeout=timeout_ms)
                    page.wait_for_timeout(150)
                    page_title = page.title()
                    checked_pages.append(
                        {
                            "path": path,
                            "url": page.url,
                            "status": response.status if response else None,
                            "title": page_title,
                        }
                    )
                    if response and response.status and response.status < 500:
                        browser.close()
                        return {
                            "ok": True,
                            "path": path,
                            "url": page.url,
                            "status": response.status,
                            "title": page_title,
                            "checked_pages": checked_pages,
                        }
                except PlaywrightError as exc:
                    checked_pages.append({"path": path, "error": str(exc)})
            browser.close()
    except Exception as exc:  # pragma: no cover - depends on local browser runtime
        return {
            "ok": False,
            "reason": f"browser_launch_failed:{exc}",
            "checked_paths": page_paths,
            "checked_pages": checked_pages,
        }

    return {
        "ok": False,
        "reason": "no_page_rendered_successfully",
        "checked_paths": page_paths,
        "checked_pages": checked_pages,
    }


def run_precheck(base_url: str, page_paths: list[str], timeout_s: int) -> dict[str, Any]:
    health = _health_probe(base_url, timeout_s)
    browser = _browser_probe(base_url, page_paths, timeout_s * 1000)
    allowed_to_continue = bool(health.get("ok")) and bool(browser.get("ok"))
    return {
        "base_url": base_url,
        "health": health,
        "browser": browser,
        "allowed_to_continue": allowed_to_continue,
        "blocked_reason": None if allowed_to_continue else _blocked_reason(health, browser),
    }


def _blocked_reason(health: dict[str, Any], browser: dict[str, Any]) -> str:
    if not health.get("ok"):
        return f"health_precheck_failed:{health.get('reason') or health.get('status_code')}"
    if not browser.get("ok"):
        return f"browser_precheck_failed:{browser.get('reason', 'unknown')}"
    return "unknown"


def _print_status(data: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    print(f"current_round_id={data['current_round_id']}")
    print(f"open_issue_count={data['open_issue_count']}")
    print(f"closed_issue_count={data['closed_issue_count']}")
    print(f"zero_open_streak={data['zero_open_streak']}")
    print(f"recent_open_issue_counts={data['recent_open_issue_counts']}")
    print(f"rounds_recorded={data['rounds_recorded']}")
    print(f"stop_allowed={str(data['stop_allowed']).lower()}")
    print(f"last_focus={data['last_focus']}")
    print(f"last_next_focus={data['last_next_focus']}")
    print(f"last_summary={data['last_summary']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Live browser + deep-fix loop scaffold.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="create the live fix loop ledgers")
    init_parser.add_argument("--root", type=Path, default=repo_root())
    init_parser.add_argument("--force", action="store_true")

    precheck_parser = subparsers.add_parser("precheck", help="verify live site + real browser availability")
    precheck_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    precheck_parser.add_argument("--timeout", type=int, default=10)
    precheck_parser.add_argument("--json", action="store_true")
    precheck_parser.add_argument("--page-path", action="append", dest="page_paths")

    append_parser = subparsers.add_parser("append-round", help="record one loop round into the ledgers")
    append_parser.add_argument("--root", type=Path, default=repo_root())
    append_parser.add_argument("--round-id", required=True)
    append_parser.add_argument("--focus", required=True)
    append_parser.add_argument("--new-high-value-issue-count", type=int, required=True)
    append_parser.add_argument("--fixed-count", type=int, required=True)
    append_parser.add_argument("--reopened-issue-count", type=int, required=True)
    append_parser.add_argument("--regression-failures", type=int, required=True)
    append_parser.add_argument("--open-issue-count", type=int, required=True)
    append_parser.add_argument("--closed-issue-count", type=int, required=True)
    append_parser.add_argument("--next-focus", required=True)
    append_parser.add_argument("--summary", required=True)
    append_parser.add_argument("--json", action="store_true")

    status_parser = subparsers.add_parser("status", help="show current loop state")
    status_parser.add_argument("--root", type=Path, default=repo_root())
    status_parser.add_argument("--json", action="store_true")
    status_parser.add_argument("--require-stop-allowed", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        paths = ensure_ledgers(args.root, force=args.force)
        print(f"Initialized {paths['issue_register']}")
        print(f"Initialized {paths['review_log']}")
        return 0

    if args.command == "precheck":
        page_paths = args.page_paths or DEFAULT_PAGE_PATHS
        result = run_precheck(args.base_url, page_paths, args.timeout)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"base_url={result['base_url']}")
            print(f"health_ok={str(result['health'].get('ok')).lower()}")
            print(f"browser_ok={str(result['browser'].get('ok')).lower()}")
            print(f"allowed_to_continue={str(result['allowed_to_continue']).lower()}")
            if result["allowed_to_continue"]:
                print(
                    "browser_probe="
                    f"{result['browser'].get('path')} "
                    f"status={result['browser'].get('status')} "
                    f"title={result['browser'].get('title')}"
                )
            else:
                print(f"blocked_reason={result['blocked_reason']}")
        return 0 if result["allowed_to_continue"] else 1

    if args.command == "append-round":
        ensure_ledgers(args.root)
        result = append_round(
            args.root,
            round_id=args.round_id,
            focus=args.focus,
            new_high_value_issue_count=args.new_high_value_issue_count,
            fixed_count=args.fixed_count,
            reopened_issue_count=args.reopened_issue_count,
            regression_failures=args.regression_failures,
            open_issue_count=args.open_issue_count,
            closed_issue_count=args.closed_issue_count,
            next_focus=args.next_focus,
            summary=args.summary,
        )
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(
                f"Recorded {result['round_id']} "
                f"zero_open_streak={result['zero_open_streak']} "
                f"stop_allowed={str(result['stop_allowed']).lower()}"
            )
        return 0

    if args.command == "status":
        status = _read_state(args.root)
        payload = {
            "current_round_id": status["current_round_id"],
            "open_issue_count": status["open_issue_count"],
            "closed_issue_count": status["closed_issue_count"],
            "zero_open_streak": status["zero_open_streak"],
            "recent_open_issue_counts": status["recent_open_issue_counts"],
            "rounds_recorded": status["rounds_recorded"],
            "stop_allowed": status["stop_allowed"],
            "last_focus": status["last_focus"],
            "last_next_focus": status["last_next_focus"],
            "last_summary": status["last_summary"],
        }
        _print_status(payload, as_json=args.json)
        if args.require_stop_allowed and not status["stop_allowed"]:
            return 1
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
