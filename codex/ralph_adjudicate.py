from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from codex.ralph_story_normalize import (
    parse_notes_payload,
    prd_story_set_hash,
    story_fingerprint,
    story_write_scope_hash,
)
from codex.ralph_truth import TruthSnapshot


@dataclass(slots=True)
class StoryDecision:
    story_id: str
    decision: str
    fingerprint: str
    write_scope_hash: str
    runtime_sentinel_hash: str
    reasons: list[str]


@dataclass(slots=True)
class AdjudicationResult:
    generated_at: str
    decisions: list[StoryDecision]

    def to_dict(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for decision in self.decisions:
            counts[decision.decision] = counts.get(decision.decision, 0) + 1
        return {
            "generated_at": self.generated_at,
            "summary": counts,
            "decisions": [asdict(item) for item in self.decisions],
        }


def _runtime_sentinel_hash(snapshot: TruthSnapshot, runtime_checks: list[str]) -> str:
    payload = []
    for name in runtime_checks:
        state = snapshot.sentinels.get(name)
        payload.append(
            {
                "name": name,
                "ok": state.ok if state else False,
                "blocked_external": state.blocked_external if state else False,
                "details": state.details if state else {},
            }
        )
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _all_runtime_checks_ok(snapshot: TruthSnapshot, runtime_checks: list[str]) -> tuple[bool, bool, list[str]]:
    ok = True
    blocked_external = False
    reasons: list[str] = []
    for name in runtime_checks:
        state = snapshot.sentinels.get(name)
        if state is None:
            ok = False
            reasons.append(f"missing_runtime_sentinel:{name}")
            continue
        if not state.ok:
            ok = False
            reasons.append(f"runtime_sentinel_failed:{name}")
        if state.blocked_external:
            blocked_external = True
    return ok, blocked_external, reasons


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def build_manifest(
    *,
    prd_payload: Mapping[str, Any],
    decisions: list[StoryDecision],
    repo_root: Path,
    baseline_commit: str | None = None,
) -> dict[str, Any]:
    decision_map = {item.story_id: item for item in decisions}
    stories: dict[str, Any] = {}
    for story in prd_payload.get("userStories") or []:
        story_id = str(story.get("id"))
        notes = parse_notes_payload(story.get("notes"))
        stories[story_id] = {
            "fingerprint": story_fingerprint(story),
            "write_scope_hash": story_write_scope_hash(repo_root, notes.get("writeScope") or []),
            "runtime_sentinel_hash": decision_map[story_id].runtime_sentinel_hash,
            "last_decision": decision_map[story_id].decision,
            "passes": bool(story.get("passes")),
        }
    return {
        "baseline_commit": baseline_commit,
        "story_set_hash": prd_story_set_hash(prd_payload),
        "stories": stories,
    }


def adjudicate_prd(
    prd_payload: dict[str, Any],
    *,
    truth_snapshot: TruthSnapshot,
    repo_root: Path,
    previous_manifest: Mapping[str, Any] | None = None,
    bootstrap_existing_true: bool = True,
    generated_at: str,
) -> tuple[dict[str, Any], AdjudicationResult]:
    previous_manifest = previous_manifest or {}
    previous_stories = dict(previous_manifest.get("stories") or {})
    decisions: list[StoryDecision] = []

    for story in prd_payload.get("userStories") or []:
        story_id = str(story.get("id"))
        notes = parse_notes_payload(story.get("notes"))
        runtime_checks = list(notes.get("runtimeChecks") or [])
        current_fingerprint = story_fingerprint(story)
        current_write_scope_hash = story_write_scope_hash(repo_root, notes.get("writeScope") or [])
        current_runtime_hash = _runtime_sentinel_hash(truth_snapshot, runtime_checks)
        sentinel_ok, blocked_external, sentinel_reasons = _all_runtime_checks_ok(truth_snapshot, runtime_checks)
        previous = previous_stories.get(story_id)
        reasons: list[str] = []
        decision = "remain_false"

        if previous:
            fingerprint_ok = previous.get("fingerprint") == current_fingerprint
            scope_ok = previous.get("write_scope_hash") == current_write_scope_hash
            if fingerprint_ok and scope_ok and sentinel_ok and bool(previous.get("passes")):
                decision = "keep_true"
                story["passes"] = True
            else:
                story["passes"] = False
                reasons.extend(sentinel_reasons)
                if not fingerprint_ok:
                    reasons.append("fingerprint_changed")
                if not scope_ok:
                    reasons.append("write_scope_changed")
                decision = "blocked_external" if blocked_external else ("regress_to_false" if previous.get("passes") else "remain_false")
        else:
            if bootstrap_existing_true and bool(story.get("passes")):
                if sentinel_ok:
                    decision = "keep_true"
                    story["passes"] = True
                    reasons.append("bootstrap_preserve_existing_true")
                else:
                    decision = "blocked_external" if blocked_external else "regress_to_false"
                    story["passes"] = False
                    reasons.extend(["bootstrap_runtime_regression", *sentinel_reasons])
            else:
                story["passes"] = False
                decision = "blocked_external" if blocked_external else "remain_false"
                reasons.extend(sentinel_reasons)

        decisions.append(
            StoryDecision(
                story_id=story_id,
                decision=decision,
                fingerprint=current_fingerprint,
                write_scope_hash=current_write_scope_hash,
                runtime_sentinel_hash=current_runtime_hash,
                reasons=reasons,
            )
        )

    return prd_payload, AdjudicationResult(generated_at=generated_at, decisions=decisions)
