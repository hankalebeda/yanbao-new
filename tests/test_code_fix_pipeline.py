"""Tests for the code-fix pipeline endpoints: synthesize-patches and scoped-pytest.

Covers:
  - synthesize-patches with fixable findings (mocked AI)
  - synthesize-patches with no fixable findings => NO_FIXABLE_FINDINGS
  - synthesize-patches with missing source bundle => 409
  - scoped-pytest with no changed files => skip
  - scoped-pytest with real test file => runs subprocess
  - AI triage rejection blocks code-fix commit path
  - patch SHA / path-traversal validation
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

import automation.promote_prep.service as promote_service
from automation.promote_prep.app import PromotePrepConfig, create_app


def _make_client(tmp_path: Path) -> TestClient:
    cfg = PromotePrepConfig(
        repo_root=tmp_path.resolve(),
        shadow_root=(tmp_path / "docs" / "_temp" / "issue_mesh_shadow").resolve(),
        runtime_root=(tmp_path / "runtime" / "issue_mesh" / "promote_prep").resolve(),
        auth_token="shadow-secret",
        redis_url="",
        queue_name="issue_mesh_shadow",
        consumer_poll_seconds=0.1,
        lease_seconds=10,
    )
    return TestClient(create_app(cfg), base_url="http://localhost")


def _seed_shadow_bundle(tmp_path: Path, run_id: str, findings: list[dict] | None = None) -> None:
    shadow_root = tmp_path / "docs" / "_temp" / "issue_mesh_shadow" / run_id
    shadow_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "generated_at": "2026-03-27T00:00:00+00:00",
        "finding_count": len(findings or []),
        "findings": findings or [],
    }
    for name in ("bundle.json", "findings_bundle.json"):
        (shadow_root / name).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    (shadow_root / "summary.md").write_text("# Summary\n", encoding="utf-8")
    (shadow_root / "candidate_writeback.md").write_text("# Candidate\n", encoding="utf-8")
    (shadow_root / "candidate_blocks.json").write_text("{}", encoding="utf-8")
    (shadow_root / "metadata.json").write_text("{}", encoding="utf-8")


# ── synthesize-patches ──────────────────────────────────────────────────


def test_synthesize_patches_no_fixable_findings(tmp_path: Path):
    """Bundle with no fix_code/fix_then_rebuild findings ⇒ NO_FIXABLE_FINDINGS."""
    run_id = "issue-mesh-20260327-901"
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "doc-correction",
                "risk_level": "P2",
                "issue_status": "stale",
                "handling_path": "execution_and_monitoring",
                "recommended_action": "update wording",
            }
        ],
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={
                "source_run_id": run_id,
                "fix_run_id": "fix-001",
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 0
    assert body["skip_reason"] == "NO_FIXABLE_FINDINGS"
    assert body["patches"] == []


def test_synthesize_patches_missing_bundle_returns_409(tmp_path: Path):
    """Request with non-existent source_run_id ⇒ 409."""
    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={
                "source_run_id": "issue-mesh-20260327-902",
                "fix_run_id": "fix-missing",
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
    assert resp.status_code == 409, resp.text


def test_synthesize_patches_with_fixable_findings(tmp_path: Path, monkeypatch):
    """Fixable findings trigger LLM call and return patches; AI is mocked."""
    run_id = "issue-mesh-20260327-903"
    target_file = tmp_path / "app" / "services" / "broken.py"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("def broken():\n    return None\n", encoding="utf-8")

    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "truth-lineage",
                "risk_level": "P1",
                "issue_status": "still_alive",
                "handling_path": "fix_code",
                "recommended_action": "Patch data lineage code",
                "evidence_refs": [],
                "ssot_refs": ["03", "04"],
                "source_task_id": "truth-lineage",
                "source_run_id": run_id,
            }
        ],
    )

    # Mock AI to return a valid patch dict
    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        lambda self, **kwargs: {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "app/services/broken.py",
                "patch_text": "def broken():\n    return 42\n",
                "explanation": "Fixed return value",
            }),
            "confidence": 0.9,
            "raw": {},
        },
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={
                "source_run_id": run_id,
                "fix_run_id": "fix-002",
                "max_fix_items": 5,
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 1
    assert body["skip_reason"] is None
    assert len(body["patches"]) == 1
    patch = body["patches"][0]
    assert patch["issue_key"] == "truth-lineage"
    assert patch["target_path"] == "app/services/broken.py"
    assert patch["valid"] is True
    assert patch["base_sha256"]  # non-empty

    # Verify manifest was persisted
    manifest = tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "code_fix" / "fix-002" / "patches.json"
    assert manifest.exists()
    persisted = json.loads(manifest.read_text(encoding="utf-8"))
    assert persisted["patch_count"] == 1


def test_synthesize_patches_path_traversal_sanitized(tmp_path: Path, monkeypatch):
    """AI returning ../../etc/passwd is rejected as TARGET_OUTSIDE_REPO."""
    run_id = "issue-mesh-20260327-904"
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "runtime-anchor",
                "handling_path": "fix_then_rebuild",
                "recommended_action": "fix",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        lambda self, **kwargs: {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "../../etc/passwd",
                "patch_text": "malicious",
                "explanation": "path traversal attempt",
            }),
            "confidence": 0.9,
            "raw": {},
        },
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-traversal"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 0
    patch = body["patches"][0]
    assert patch["target_path"] == ""
    assert patch["valid"] is False
    assert patch["base_sha256"] == ""
    assert "TARGET_OUTSIDE_REPO" in patch["explanation"]


def test_synthesize_patches_rejects_diff_style_patch_text(tmp_path: Path, monkeypatch):
    """AI diff output is rejected because writeback expects full file content."""
    run_id = "issue-mesh-20260327-910"
    target_file = tmp_path / "app" / "services" / "broken.py"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("def broken():\n    return None\n", encoding="utf-8")

    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "truth-lineage",
                "handling_path": "fix_code",
                "recommended_action": "fix broken return",
                "evidence_refs": ["app/services/broken.py"],
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        lambda self, **kwargs: {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "app/services/broken.py",
                "patch_text": "*** Begin Patch\n*** Update File: app/services/broken.py\n@@\n-def broken():\n-    return None\n+def broken():\n+    return 42\n*** End Patch",
                "explanation": "returned apply_patch diff",
            }),
            "confidence": 0.9,
            "raw": {},
        },
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-diff-style"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 0
    patch = body["patches"][0]
    assert patch["target_path"] == ""
    assert patch["valid"] is False
    assert "PATCH_TEXT_NOT_FULL_FILE_CONTENT" in patch["explanation"]


def test_synthesize_patches_rejects_ungrounded_new_typescript_file(tmp_path: Path, monkeypatch):
    """New TypeScript target is rejected when evidence is Python-only."""
    run_id = "issue-mesh-20260327-911"
    evidence_file = tmp_path / "app" / "services" / "report_task.py"
    evidence_file.parent.mkdir(parents=True, exist_ok=True)
    evidence_file.write_text("def report_task():\n    return 'ok'\n", encoding="utf-8")

    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "fr06-failure-semantics",
                "handling_path": "fix_code",
                "recommended_action": "fix failure semantics",
                "evidence_refs": ["app/services/report_task.py"],
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        lambda self, **kwargs: {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "app/api/v1/reports/generate/route.ts",
                "patch_text": "export function handler() { return 'nope' }\n",
                "explanation": "hallucinated next route",
            }),
            "confidence": 0.9,
            "raw": {},
        },
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-ts-hallucination"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 0
    patch = body["patches"][0]
    assert patch["target_path"] == ""
    assert patch["valid"] is False
    assert "TARGET_SUFFIX_UNGROUNDED:.ts" in patch["explanation"]


def test_synthesize_patches_supports_absolute_evidence_refs(tmp_path: Path, monkeypatch):
    """Absolute evidence_refs (including trailing hints) should be loaded into file_contexts."""
    run_id = "issue-mesh-20260327-905"
    target_file = tmp_path / "app" / "services" / "anchor_probe.py"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("def probe():\n    return 'old'\n", encoding="utf-8")

    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "runtime-anchor",
                "handling_path": "fix_then_rebuild",
                "recommended_action": "Patch runtime-anchor code",
                "evidence_refs": [f"{target_file.resolve()} (from runtime evidence)"],
            }
        ],
    )

    captured_prompts: list[str] = []

    def _fake_ai(self, **kwargs):
        captured_prompts.append(str(kwargs.get("prompt") or ""))
        return {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "app/services/anchor_probe.py",
                "patch_text": "def probe():\n    return 'new'\n",
                "explanation": "patched",
            }),
            "confidence": 0.9,
            "raw": {},
        }

    monkeypatch.setattr(promote_service.PromotePrepService, "_run_ai_triage_prompt", _fake_ai)

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-abs-ref"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 1
    assert captured_prompts
    assert '"path": "app/services/anchor_probe.py"' in captured_prompts[0]


def test_codex_cli_fallback_writes_utf8_stdin(tmp_path: Path, monkeypatch):
    """Codex CLI fallback must send UTF-8 stdin on Windows-friendly code paths."""
    captured: dict[str, object] = {}
    canonical_provider = "newapi-192.168.232.141-3000"
    provider_dir = tmp_path / "ai-api" / "codex" / canonical_provider
    provider_dir.mkdir(parents=True, exist_ok=True)

    def _fake_run(args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        output_path = Path(args[-2])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps({"decision": "freeze", "reason": "mocked", "confidence": 0.1}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    prompt = "contains utf8 user prompt"
    system_prompt = "system prompt"
    monkeypatch.setattr(shutil, "which", lambda name: "codex" if name == "codex" else None)
    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", canonical_provider)

    result = promote_service._run_codex_cli_fallback(prompt, system_prompt, tmp_path)

    assert result["provider_name"] == "codex-cli"
    assert captured["args"] == [
        "codex",
        "exec",
        "-m",
        promote_service.DEFAULT_TRIAGE_MODEL,
        "--sandbox",
        "read-only",
        "--ephemeral",
        "-o",
        captured["args"][-2],
        "-",
    ]
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["env"]["CODEX_HOME"] == str(provider_dir)
    assert kwargs["input"] == f"[System]\n{system_prompt}\n\n[User]\n{prompt}"
    assert kwargs["text"] is True
    assert kwargs["encoding"] == "utf-8"
    assert kwargs["errors"] == "strict"
    assert not Path(captured["args"][-2]).exists()


# ── scoped-pytest ───────────────────────────────────────────────────────


def test_scoped_pytest_no_changed_files(tmp_path: Path):
    """Empty changed_files list ⇒ skip with NO_CHANGED_FILES."""
    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/scoped-pytest",
            json={
                "fix_run_id": "fix-pytest-01",
                "changed_files": [],
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["passed"] is None
    assert body["skip_reason"] == "NO_CHANGED_FILES"
    assert body.get("verify_status") == "not_verified"


def test_scoped_pytest_no_matching_tests(tmp_path: Path):
    """Changed file has no matching test file ⇒ NO_RELATED_TESTS_FOUND."""
    # Create the changed file but no test file
    (tmp_path / "app" / "services").mkdir(parents=True, exist_ok=True)
    (tmp_path / "app" / "services" / "unique_no_match_xyz.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "tests").mkdir(parents=True, exist_ok=True)

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/scoped-pytest",
            json={
                "fix_run_id": "fix-pytest-02",
                "changed_files": ["app/services/unique_no_match_xyz.py"],
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["passed"] is None
    assert body["skip_reason"] == "NO_RELATED_TESTS_FOUND"
    assert body.get("verify_status") == "not_verified"


def test_scoped_pytest_runs_matching_test(tmp_path: Path):
    """Changed test file is passed directly ⇒ pytest runs it."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    # Create a trivial passing test
    (tests_dir / "test_trivial_pass.py").write_text(
        "def test_ok():\n    assert 1 + 1 == 2\n",
        encoding="utf-8",
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/scoped-pytest",
            json={
                "fix_run_id": "fix-pytest-03",
                "changed_files": ["tests/test_trivial_pass.py"],
                "timeout_seconds": 30,
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["passed"] is True
    assert body["skip_reason"] is None

    # Verify result was persisted
    result_file = (
        tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "code_fix" / "fix-pytest-03" / "pytest_result.json"
    )
    assert result_file.exists()


def test_scoped_pytest_detects_failing_test(tmp_path: Path):
    """Failing test ⇒ passed=False."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / "test_trivial_fail.py").write_text(
        "def test_fail():\n    assert 1 == 2, 'intentional fail'\n",
        encoding="utf-8",
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/scoped-pytest",
            json={
                "fix_run_id": "fix-pytest-04",
                "changed_files": ["tests/test_trivial_fail.py"],
                "timeout_seconds": 30,
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["passed"] is False
    assert body["skip_reason"] is None


# ── triage rejection blocks code-fix ────────────────────────────────────


def test_triage_rejection_blocks_code_fix_writeback(tmp_path: Path, monkeypatch):
    """When AI triage returns freeze for a code-fix patch, auto_commit must be False."""
    target = tmp_path / "app" / "services" / "patched.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_writeback_triage",
        lambda self, **kwargs: {"decision": "freeze", "reason": "unsafe_patch", "confidence": 0.7, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/writeback",
            json={
                "run_id": "kestra-cfix-001",
                "workflow_id": "yanbao_issue_mesh_code_fix_wave",
                "layer": "code-fix",
                "target_path": "app/services/patched.py",
                "patch_text": "after\n",
                "base_sha256": promote_service._sha256_text("before\n"),
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
                "preview_summary": {"conflict": False},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["decision"] == "freeze"
    assert body["auto_commit"] is False


def test_synthesize_patches_max_fix_items_limit(tmp_path: Path, monkeypatch):
    """max_fix_items=1 limits to first fixable finding only."""
    run_id = "issue-mesh-20260327-905"
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": f"issue-{i}",
                "handling_path": "fix_code",
                "recommended_action": f"fix item {i}",
            }
            for i in range(5)
        ],
    )

    call_count = 0

    def mock_ai(self, **kwargs):
        nonlocal call_count
        call_count += 1
        return {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "",
                "patch_text": "",
                "explanation": "cannot fix",
            }),
            "confidence": 0.5,
            "raw": {},
        }

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        mock_ai,
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={
                "source_run_id": run_id,
                "fix_run_id": "fix-limit",
                "max_fix_items": 1,
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    assert call_count == 1
    assert resp.json()["total_findings"] == 1


def test_synthesize_patches_auth_required(tmp_path: Path):
    """Missing auth header ⇒ 401/403."""
    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={
                "source_run_id": "issue-mesh-20260327-906",
                "fix_run_id": "fix-auth",
            },
        )
    assert resp.status_code in (401, 403), resp.text


def test_synthesize_patches_ai_returns_empty_target_path(tmp_path: Path, monkeypatch):
    """AI returning empty target_path ⇒ patch recorded with empty target, valid=False."""
    run_id = "issue-mesh-20260327-907"
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "empty-target",
                "handling_path": "fix_code",
                "recommended_action": "fix something",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        lambda self, **kwargs: {
            "decision": "allow",
            "reason": json.dumps({
                "target_path": "",
                "patch_text": "some patch",
                "explanation": "AI could not locate file",
            }),
            "confidence": 0.3,
            "raw": {},
        },
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-empty-target"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Empty target_path ⇒ valid=False, patch_count counts only valid
    assert body["patch_count"] == 0
    assert len(body["patches"]) == 1
    patch = body["patches"][0]
    assert patch["target_path"] == ""
    assert patch["valid"] is False


def test_synthesize_patches_ai_malformed_json(tmp_path: Path, monkeypatch):
    """AI returning non-JSON reason ⇒ graceful degradation, no 500."""
    run_id = "issue-mesh-20260327-908"
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "malformed",
                "handling_path": "fix_code",
                "recommended_action": "fix",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        lambda self, **kwargs: {
            "decision": "allow",
            "reason": "this is not valid json at all {{{",
            "confidence": 0.1,
            "raw": {},
        },
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-malformed"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Should degrade gracefully — patch present but invalid (empty target)
    assert body["patch_count"] == 0
    assert len(body["patches"]) == 1
    assert body["patches"][0]["valid"] is False


def test_synthesize_patches_ai_exception(tmp_path: Path, monkeypatch):
    """AI call raising exception ⇒ AI_SYNTHESIS_FAILED, no 500."""
    run_id = "issue-mesh-20260327-909"
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "ai-crash",
                "handling_path": "fix_code",
                "recommended_action": "fix",
            }
        ],
    )

    def _raise(**kwargs):
        raise RuntimeError("LLM connection timeout")

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_run_ai_triage_prompt",
        _raise,
    )

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/triage/synthesize-patches",
            json={"source_run_id": run_id, "fix_run_id": "fix-ai-crash"},
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["patch_count"] == 0
    assert len(body["patches"]) == 1
    patch = body["patches"][0]
    assert patch["target_path"] == ""
    assert patch["valid"] is False
    assert "AI_SYNTHESIS_FAILED" in patch.get("explanation", "")
