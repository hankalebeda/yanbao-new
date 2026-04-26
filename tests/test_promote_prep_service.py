from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from fastapi.testclient import TestClient

import automation.promote_prep.service as promote_service
from automation.promote_prep.app import PromotePrepConfig, create_app
from automation.promote_prep.service import CURRENT_PROGRESS_DOC
from app.core.config import settings as app_settings


def _make_client(tmp_path: Path, *, promote_target_mode: str | None = "doc22") -> TestClient:
    state_path = tmp_path / "automation" / "control_plane" / "current_state.json"
    if promote_target_mode is not None and not state_path.exists():
        _write_infra_state(tmp_path, promote_target_mode)
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


def _seed_terminal_shards(tmp_path: Path, run_id: str) -> None:
    for index in range(1, 13):
        shard_root = tmp_path / "runtime" / "issue_mesh" / run_id / f"shard_{index:02d}"
        shard_root.mkdir(parents=True, exist_ok=True)
        (shard_root / "result.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "shard_id": f"shard_{index:02d}",
                    "status": "COMPLETED",
                    "findings": [],
                    "shadow_path": f"docs/_temp/issue_mesh_shadow/{run_id}/shard_{index:02d}.md",
                    "error": None,
                    "started_at": "2026-03-27T00:00:00+00:00",
                    "finished_at": "2026-03-27T00:05:00+00:00",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )


def _seed_terminal_shards_legacy_runs(tmp_path: Path, run_id: str) -> None:
    for index in range(1, 13):
        shard_root = tmp_path / "runtime" / "issue_mesh" / "runs" / run_id / f"shard_{index:02d}"
        shard_root.mkdir(parents=True, exist_ok=True)
        (shard_root / "result.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "shard_id": f"shard_{index:02d}",
                    "status": "COMPLETED",
                    "findings": [],
                    "shadow_path": f"docs/_temp/issue_mesh_shadow/{run_id}/shard_{index:02d}.md",
                    "error": None,
                    "started_at": "2026-03-27T00:00:00+00:00",
                    "finished_at": "2026-03-27T00:05:00+00:00",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )


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


def test_promote_prep_direct_gateway_uses_extended_timeout(tmp_path: Path, monkeypatch) -> None:
    provider_dir = tmp_path / "ai-api" / "codex" / "newapi-192.168.232.141-3000-stable"
    provider_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "key.txt").write_text("http://192.168.232.141:3000/v1\nsk-gateway\n", encoding="utf-8")

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
    service = promote_service.PromotePrepService(cfg)
    monkeypatch.setattr(app_settings, "promote_prep_gateway_timeout_seconds", 180.0)
    monkeypatch.setattr(app_settings, "codex_api_timeout_seconds", 300.0)

    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"output_text": '{"decision":"allow","reason":"ok","confidence":0.9}'}

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: float):
        captured["url"] = url
        captured["timeout"] = timeout
        captured["model"] = json.get("model")
        return FakeResponse()

    monkeypatch.setattr(promote_service.httpx, "post", fake_post)

    result = service._run_ai_triage_prompt(prompt="hello", system_prompt="system")

    assert captured["url"] == "http://192.168.232.141:3000/responses"
    assert captured["timeout"] == 180.0
    assert captured["model"] == promote_service.DEFAULT_TRIAGE_MODEL
    assert result["provider_name"] == "newapi-192.168.232.141-3000-stable"
    assert result["pool_level"] == "direct_gateway"


def test_promote_prep_direct_gateway_timeout_is_capped_by_total_budget(tmp_path: Path, monkeypatch) -> None:
    provider_dir = tmp_path / "ai-api" / "codex" / "newapi-192.168.232.141-3000-stable"
    provider_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "key.txt").write_text("http://192.168.232.141:3000/v1\nsk-gateway\n", encoding="utf-8")

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
    service = promote_service.PromotePrepService(cfg)
    monkeypatch.setattr(app_settings, "promote_prep_gateway_timeout_seconds", 180.0)
    monkeypatch.setattr(app_settings, "codex_api_timeout_seconds", 60.0)

    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"output_text": '{"decision":"allow","reason":"ok","confidence":0.9}'}

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: float):
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(promote_service.httpx, "post", fake_post)

    service._run_ai_triage_prompt(prompt="hello", system_prompt="system")

    assert abs(float(captured["timeout"]) - 60.0) < 0.1


def test_promote_prep_ai_triage_shares_total_budget_across_provider_and_cli(tmp_path: Path, monkeypatch) -> None:
    provider_dir = tmp_path / "ai-api" / "codex" / "newapi-192.168.232.141-3000-stable"
    provider_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "key.txt").write_text("http://192.168.232.141:3000/v1\nsk-gateway\n", encoding="utf-8")

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
    service = promote_service.PromotePrepService(cfg)
    monkeypatch.setattr(app_settings, "promote_prep_gateway_timeout_seconds", 180.0)
    monkeypatch.setattr(app_settings, "codex_api_timeout_seconds", 60.0)

    class Clock:
        now = 0.0

    captured: dict[str, float] = {}

    def fake_perf_counter() -> float:
        return Clock.now

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: float):
        captured["gateway_timeout"] = timeout
        Clock.now = 45.0
        raise RuntimeError("gateway failed")

    async def fake_wait_for(coro, timeout: float):
        captured["provider_timeout"] = timeout
        Clock.now = 55.0
        coro.close()
        raise TimeoutError("provider timeout")

    def fake_discover(root=None):
        return [object()]

    class FakeCodexAPIClient:
        def __init__(self, provider_specs):
            self.provider_specs = provider_specs

        async def analyze(self, **kwargs):
            return {"response": '{"decision":"allow","reason":"provider","confidence":0.9}'}

        async def close(self):
            return None

    def fake_cli(prompt: str, system_prompt: str, repo_root: Path, *, timeout_seconds: float):
        captured["cli_timeout"] = timeout_seconds
        return {
            "response": '{"decision":"allow","reason":"cli","confidence":0.9}',
            "provider_name": "codex-cli",
            "model": promote_service.DEFAULT_TRIAGE_MODEL,
            "reasoning_effort": "high",
            "pool_level": "codex_cli_fallback",
            "elapsed_s": 1.0,
        }

    monkeypatch.setattr(promote_service.time, "perf_counter", fake_perf_counter)
    monkeypatch.setattr(promote_service.httpx, "post", fake_post)
    monkeypatch.setattr(promote_service.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setattr(promote_service, "_run_codex_cli_fallback", fake_cli)
    monkeypatch.setattr("app.services.codex_client.discover_codex_provider_specs", fake_discover)
    monkeypatch.setattr("app.services.codex_client.CodexAPIClient", FakeCodexAPIClient)

    result = service._run_ai_triage_prompt(prompt="hello", system_prompt="system")

    assert captured["gateway_timeout"] == 60.0
    assert captured["provider_timeout"] == 15.0
    assert captured["cli_timeout"] == 5.0
    assert result["provider_name"] == "codex-cli"


def test_promote_prep_writes_shadow_outputs(tmp_path: Path):
    payload = {
        "run_id": "issue-mesh-20260327-101",
        "summary_markdown": "# Summary\n\ntext\n",
        "findings_bundle": {
            "run_id": "issue-mesh-20260327-101",
            "generated_at": "2026-03-27T00:00:00+00:00",
            "finding_count": 1,
            "findings": [
                {
                    "issue_key": "truth-lineage",
                    "title": "truth-lineage",
                    "risk_level": "P2",
                    "issue_status": "narrow_required",
                    "handling_path": "execution_and_monitoring",
                    "recommended_action": "review it",
                    "evidence_refs": ["runtime/issue_mesh/issue-mesh-20260327-101/summary.md"],
                    "ssot_refs": [],
                    "source_task_id": "truth-lineage",
                    "source_run_id": "issue-mesh-20260327-101",
                }
            ],
        },
    }

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/shadow",
            json=payload,
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert Path(data["summary_path"]).exists()
        assert Path(data["bundle_path"]).exists()
        assert Path(data["legacy_bundle_path"]).exists()
        assert Path(data["candidate_writeback_path"]).exists()
        assert json.loads(Path(data["bundle_path"]).read_text(encoding="utf-8"))["finding_count"] == 1

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["metrics"]["failure_count"] == 0


def test_promote_prep_async_intent_writes_shadow_outputs(tmp_path: Path):
    payload = {
        "run_id": "issue-mesh-20260327-102",
        "summary_markdown": "# Summary\n\ntext\n",
        "findings_bundle": {
            "run_id": "issue-mesh-20260327-102",
            "generated_at": "2026-03-27T00:00:00+00:00",
            "finding_count": 1,
            "findings": [
                {
                    "issue_key": "runtime-anchor",
                    "title": "runtime-anchor",
                    "risk_level": "P1",
                    "issue_status": "still_alive",
                    "handling_path": "execution_and_monitoring",
                    "recommended_action": "review it",
                    "evidence_refs": ["runtime/issue_mesh/issue-mesh-20260327-102/summary.md"],
                    "ssot_refs": ["03", "04"],
                    "source_task_id": "runtime-anchor",
                    "source_run_id": "issue-mesh-20260327-102",
                }
            ],
        },
        "logical_target": "issue_mesh_shadow",
    }

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/intents",
            json=payload,
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 200, response.text
        intent_id = response.json()["intent_id"]

        status = None
        for _ in range(20):
            detail = client.get(
                f"/v1/intents/{intent_id}",
                headers={"Authorization": "Bearer shadow-secret"},
            )
            assert detail.status_code == 200, detail.text
            status = detail.json()["status"]
            if status == "written":
                break
            time.sleep(0.1)

        assert status == "written"
        shadow_paths = detail.json()["shadow_paths"]
        assert Path(shadow_paths["bundle_path"]).exists()
        assert Path(shadow_paths["legacy_bundle_path"]).exists()
        assert Path(shadow_paths["candidate_blocks_path"]).exists()


def test_promote_prep_intent_wait_for_completion_returns_terminal_status(tmp_path: Path):
    payload = {
        "run_id": "issue-mesh-20260327-103",
        "summary_markdown": "# Summary\n\ntext\n",
        "findings_bundle": {
            "run_id": "issue-mesh-20260327-103",
            "generated_at": "2026-03-27T00:00:00+00:00",
            "finding_count": 1,
            "findings": [{"issue_key": "truth-lineage", "title": "x"}],
        },
        "logical_target": "issue_mesh_shadow",
    }

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/intents",
            json=payload,
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 200, response.text
        intent_id = response.json()["intent_id"]

        waited = client.get(
            f"/v1/intents/{intent_id}?wait_for_completion=true&wait_timeout_seconds=5",
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert waited.status_code == 200, waited.text
        assert waited.json()["status"] == "written"
        assert waited.json()["shadow_paths"]["shadow_root"].endswith("issue-mesh-20260327-103")


def test_promote_prep_prepares_status_note_patch(tmp_path: Path):
    run_id = "issue-mesh-20260327-201"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\nexisting\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "truth-lineage",
                "risk_level": "P1",
                "issue_status": "still_alive",
                "recommended_action": "补齐证据",
            }
        ],
    )

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["layer"] == "status-note"
        assert payload["target_anchor"] == "current-writeback-detail"
        assert payload["idempotency_key"] == f"issue-mesh:{run_id}:status-note:current-writeback-detail"
        assert payload["request_id"] == f"req-{run_id}-status-note-current-writeback-detail"
        assert payload["skip_commit"] is False
        assert run_id in payload["patch_text"]
        assert "- layer: `status-note`" in payload["patch_text"]
        assert "- target_anchor: `current-writeback-detail`" in payload["patch_text"]
        assert "- patch_timestamp: `2026-03-27T00:00:00+00:00`" in payload["patch_text"]
        assert payload["semantic_fingerprint"] in payload["patch_text"]
        assert payload["shadow_snapshot"]["run_id"] == run_id


def test_promote_prep_status_note_skips_when_semantic_fingerprint_already_present(tmp_path: Path):
    run_id = "issue-mesh-20260327-201"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\n- semantic_fingerprint: `seed`\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )
    _seed_terminal_shards(tmp_path, run_id)
    findings = [
        {
            "issue_key": "truth-lineage",
            "risk_level": "P1",
            "issue_status": "still_alive",
            "recommended_action": "补齐证据",
        }
    ]
    _seed_shadow_bundle(tmp_path, run_id, findings=findings)
    bundle = {"run_id": run_id, "generated_at": "2026-03-27T00:00:00+00:00", "finding_count": 1, "findings": findings}
    fingerprint = promote_service._bundle_semantic_fingerprint(
        bundle,
        layer="status-note",
        target_anchor="current-writeback-detail",
    )
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\n"
        f"- semantic_fingerprint: `{fingerprint}`\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["skip_commit"] is True
    assert response.json()["skip_reason"] == "SEMANTIC_FINGERPRINT_ALREADY_PRESENT"


def test_promote_prep_status_note_requires_shadow_bundle(tmp_path: Path):
    run_id = "issue-mesh-20260327-202"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 409, response.text
        assert response.json()["detail"] == "SHADOW_BUNDLE_NOT_FOUND"


def test_promote_prep_status_note_accepts_legacy_runtime_runs_path(tmp_path: Path):
    run_id = "issue-mesh-20260327-202"
    doc22 = tmp_path / CURRENT_PROGRESS_DOC
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 鍐欏洖鏄庣粏\n\nexisting\n\n## 4. 涓撻」澶嶆牳涓庡悗缁紭鍏堢骇\n",
        encoding="utf-8",
    )
    _seed_terminal_shards_legacy_runs(tmp_path, run_id)
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "runtime-anchor",
                "risk_level": "P1",
                "issue_status": "still_alive",
                "recommended_action": "fix runtime anchor",
            }
        ],
    )

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["skip_commit"] is False
    assert run_id in response.json()["patch_text"]


def test_promote_prep_status_note_patch_is_stable_for_same_run(tmp_path: Path):
    run_id = "issue-mesh-20260327-207"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\nexisting\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[{"issue_key": "issue-registry", "risk_level": "P1", "issue_status": "narrow_required", "recommended_action": "fill registry"}],
    )

    with _make_client(tmp_path) as client:
        first = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        time.sleep(0.01)
        second = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["patch_text"] == second.json()["patch_text"]


def test_promote_prep_current_layer_prepare_is_disabled_by_default(tmp_path: Path):
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n", encoding="utf-8")

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/current-layer",
            json={
                "run_id": "issue-mesh-20260327-203",
                "enabled": False,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["skip_commit"] is True
        assert payload["skip_reason"] == "CURRENT_LAYER_PROMOTE_DISABLED"


def test_promote_prep_current_layer_prepare_replaces_anchors_when_enabled(tmp_path: Path):
    run_id = "issue-mesh-20260327-204"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        (
            "# 22\n\n"
            "## 2. 当前问题判定板\n\n"
            "### 2.1 当前仍存活问题\nold-21\n\n"
            "### 2.2 已过时项\nkeep-22\n\n"
            "### 2.3 当前残余真实项\nold-23\n\n"
            "## 4. 专项复核与后续优先级\n\n"
            "### 4.5 后续优先级\nold-45\n\n"
            "## 5. 附录\nappendix\n"
        ),
        encoding="utf-8",
    )
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {"issue_key": "runtime-anchor", "risk_level": "P1", "issue_status": "still_alive", "recommended_action": "fix runtime"},
            {"issue_key": "issue-registry", "risk_level": "P2", "issue_status": "narrow_required", "recommended_action": "fill registry"},
        ],
    )

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/current-layer",
            json={
                "run_id": run_id,
                "enabled": True,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "READY"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["skip_commit"] is False
        assert payload["target_anchor"] == "2.1|2.3|4.5"
        assert payload["idempotency_key"] == f"issue-mesh:{run_id}:current-layer:2.1|2.3|4.5"
        assert payload["request_id"] == f"req-{run_id}-current-layer-2.1-2.3-4.5"
        assert payload["semantic_fingerprint"] in payload["patch_text"]
        assert run_id in payload["patch_text"]
        assert "layer=current-layer" in payload["patch_text"]
        assert "old-21" not in payload["patch_text"]
        assert "old-23" not in payload["patch_text"]
        assert "old-45" not in payload["patch_text"]
        assert "### 2.2 已过时项\nkeep-22" in payload["patch_text"]
        assert "## 5. 附录\nappendix" in payload["patch_text"]
        assert payload["shadow_snapshot"]["run_id"] == run_id


def test_promote_prep_triage_allows_p1_status_note_auto_commit(tmp_path: Path, monkeypatch):
    run_id = "issue-mesh-20260327-210"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\nexisting\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "runtime-anchor",
                "risk_level": "P1",
                "issue_status": "still_alive",
                "recommended_action": "fix runtime anchor",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_triage",
        lambda self, **kwargs: {"decision": "allow", "reason": "safe_to_commit", "confidence": 0.91, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        prepare = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        triage = client.post(
            "/v1/triage",
            json={
                "run_id": run_id,
                "layer": "status-note",
                "target_path": prepare.json()["target_path"],
                "target_anchor": prepare.json()["target_anchor"],
                "patch_text": prepare.json()["patch_text"],
                "base_sha256": prepare.json()["base_sha256"],
                "semantic_fingerprint": prepare.json()["semantic_fingerprint"],
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert prepare.status_code == 200, prepare.text
    assert triage.status_code == 200, triage.text
    assert triage.json()["decision"] == "allow"
    assert triage.json()["auto_commit"] is True
    assert triage.json()["decision_source"] == "ai"
    assert triage.json()["triage_record_id"] == f"{run_id}__status-note"


def test_promote_prep_triage_auto_allows_shadow_only_status_note_without_p0(tmp_path: Path, monkeypatch):
    run_id = "issue-mesh-20260327-219"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\nexisting\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "issue-registry",
                "risk_level": "P1",
                "issue_status": "narrow_required",
                "recommended_action": "narrow wording",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_triage",
        lambda self, **kwargs: {"decision": "freeze", "reason": "conservative_freeze", "confidence": 0.88, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        prepare = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        triage = client.post(
            "/v1/triage",
            json={
                "run_id": run_id,
                "layer": "status-note",
                "target_path": prepare.json()["target_path"],
                "target_anchor": prepare.json()["target_anchor"],
                "patch_text": prepare.json()["patch_text"],
                "base_sha256": prepare.json()["base_sha256"],
                "semantic_fingerprint": prepare.json()["semantic_fingerprint"],
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert prepare.status_code == 200, prepare.text
    assert triage.status_code == 200, triage.text
    assert triage.json()["decision"] == "allow"
    assert triage.json()["auto_commit"] is True
    assert triage.json()["decision_source"] == "policy_override"
    assert triage.json()["reason"] == "STATUS_NOTE_AUTO_ALLOW"


def test_promote_prep_triage_freezes_p0_even_when_ai_allows(tmp_path: Path, monkeypatch):
    run_id = "issue-mesh-20260327-211"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n<a id=\"current-writeback-detail\"></a>\n## 3. 写回明细\n\nexisting\n\n## 4. 专项复核与后续优先级\n",
        encoding="utf-8",
    )
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "truth-lineage",
                "risk_level": "P0",
                "issue_status": "still_alive",
                "recommended_action": "freeze writeback",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_triage",
        lambda self, **kwargs: {"decision": "allow", "reason": "unsafe_fake_allow", "confidence": 0.95, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        prepare = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        triage = client.post(
            "/v1/triage",
            json={
                "run_id": run_id,
                "layer": "status-note",
                "target_path": prepare.json()["target_path"],
                "target_anchor": prepare.json()["target_anchor"],
                "patch_text": prepare.json()["patch_text"],
                "base_sha256": prepare.json()["base_sha256"],
                "semantic_fingerprint": prepare.json()["semantic_fingerprint"],
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert triage.status_code == 200, triage.text
    assert triage.json()["decision"] == "freeze"
    assert triage.json()["auto_commit"] is False
    assert triage.json()["decision_source"] == "policy_override"
    assert triage.json()["reason"] == "P0_FINDINGS_REQUIRE_FREEZE"
    assert triage.json()["triage_record_id"] == f"{run_id}__status-note"


def test_promote_prep_triage_allows_low_risk_doc_correction_when_ai_returns_shadow_only(tmp_path: Path, monkeypatch):
    run_id = "issue-mesh-20260327-212"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n\nold line\n", encoding="utf-8")
    _seed_shadow_bundle(
        tmp_path,
        run_id,
        findings=[
            {
                "issue_key": "doc-correction",
                "risk_level": "P2",
                "issue_status": "stale",
                "recommended_action": "narrow stale wording",
            }
        ],
    )

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_triage",
        lambda self, **kwargs: {"decision": "shadow_only", "reason": "conservative", "confidence": 0.8, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        triage = client.post(
            "/v1/triage",
            json={
                "run_id": run_id,
                "layer": "doc-correction",
                "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                "target_anchor": "3.5.3",
                "patch_text": "# 22\n\nnew line\n",
                "base_sha256": promote_service._sha256_text("# 22\n\nold line\n"),
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert triage.status_code == 200, triage.text
    assert triage.json()["decision"] == "allow"
    assert triage.json()["auto_commit"] is True
    assert triage.json()["decision_source"] == "policy_override"
    assert triage.json()["reason"] == "LOW_RISK_DOC_CORRECTION_AUTO_ALLOW"
    assert triage.json()["triage_record_id"] == f"{run_id}__doc-correction"


def test_promote_prep_generic_writeback_triage_allows_high_risk_path_on_ai_allow(tmp_path: Path, monkeypatch):
    target = tmp_path / "app" / "services" / "auto_patch.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_writeback_triage",
        lambda self, **kwargs: {"decision": "allow", "reason": "approved_high_risk", "confidence": 0.93, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        triage = client.post(
            "/v1/triage/writeback",
            json={
                "run_id": "kestra-report-001",
                "workflow_id": "yanbao_report_writeback_orchestration",
                "layer": "report-code-writeback",
                "target_path": "app/services/auto_patch.py",
                "patch_text": "after\n",
                "base_sha256": promote_service._sha256_text("before\n"),
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
                "preview_summary": {"conflict": False},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert triage.status_code == 200, triage.text
    body = triage.json()
    assert body["decision"] == "allow"
    assert body["auto_commit"] is True
    assert body["target_risk"] == "high"
    assert body["relative_target_path"] == "app/services/auto_patch.py"
    assert body["run_id"] == "kestra-report-001"
    assert body["triage_record_id"] == "kestra-report-001__report-code-writeback-writeback"


def test_promote_prep_generic_writeback_triage_freezes_high_risk_when_confidence_low(tmp_path: Path, monkeypatch):
    target = tmp_path / "tests" / "test_auto_patch.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_writeback_triage",
        lambda self, **kwargs: {"decision": "allow", "reason": "low_confidence", "confidence": 0.2, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        triage = client.post(
            "/v1/triage/writeback",
            json={
                "workflow_id": "yanbao_issue_mesh_status_note_promote",
                "layer": "code-writeback",
                "target_path": "tests/test_auto_patch.py",
                "patch_text": "after\n",
                "base_sha256": promote_service._sha256_text("before\n"),
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "READY"},
                "preview_summary": {"conflict": False},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert triage.status_code == 200, triage.text
    body = triage.json()
    assert body["decision"] == "freeze"
    assert body["auto_commit"] is False
    assert body["reason"] == "HIGH_RISK_CONFIDENCE_TOO_LOW"
    assert body["decision_source"] == "policy_override"


def test_promote_prep_generic_writeback_triage_freezes_on_preview_conflict(tmp_path: Path, monkeypatch):
    target = tmp_path / "automation" / "probe.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")

    monkeypatch.setattr(
        promote_service.PromotePrepService,
        "_call_ai_writeback_triage",
        lambda self, **kwargs: {"decision": "allow", "reason": "would_allow_without_conflict", "confidence": 0.99, "raw": {}},
    )

    with _make_client(tmp_path) as client:
        triage = client.post(
            "/v1/triage/writeback",
            json={
                "workflow_id": "yanbao_report_writeback_orchestration",
                "target_path": "automation/probe.py",
                "patch_text": "after\n",
                "base_sha256": promote_service._sha256_text("before\n"),
                "preview_summary": {"conflict": True},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert triage.status_code == 200, triage.text
    body = triage.json()
    assert body["decision"] == "freeze"
    assert body["auto_commit"] is False
    assert body["reason"] == "PREVIEW_CONFLICT"
    assert body["decision_source"] == "policy_override"


def test_promote_prep_current_layer_requires_shadow_bundle_when_active(tmp_path: Path):
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text(
        "# 22\n\n## 2. 当前问题判定板\n\n### 2.1 当前仍存活问题\nold-21\n\n### 2.3 当前残余真实项\nold-23\n\n## 4. 专项复核与后续优先级\n\n### 4.5 后续优先级\nold-45\n",
        encoding="utf-8",
    )

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/promote/current-layer",
            json={
                "run_id": "issue-mesh-20260327-205",
                "enabled": True,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "READY"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert response.status_code == 409, response.text
        assert response.json()["detail"] == "SHADOW_BUNDLE_NOT_FOUND"


def test_promote_prep_verifies_rollback_acceptance(tmp_path: Path):
    run_id = "issue-mesh-20260327-206"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n\nstable\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(tmp_path, run_id)

    with _make_client(tmp_path) as client:
        prepare = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "audit_context": {"public_runtime_status": "DEGRADED"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert prepare.status_code == 200, prepare.text
        expected_base_sha256 = prepare.json()["base_sha256"]

        verify = client.post(
            "/v1/promote/rollback-acceptance",
            json={
                "run_id": run_id,
                "layer": "status-note",
                "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                "expected_base_sha256": expected_base_sha256,
                "expected_shadow_snapshot": prepare.json()["shadow_snapshot"],
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert verify.status_code == 200, verify.text
        assert verify.json()["acceptance_passed"] is True


def _write_infra_state(tmp_path: Path, mode: str = "infra") -> None:
    """Write a control plane state file with the given promote_target_mode."""
    state_path = tmp_path / "automation" / "control_plane" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"_schema": "infra_promote_v1", "promote_target_mode": mode}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_guard_audit(tmp_path: Path) -> list[dict]:
    audit_path = tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "guard_audit.jsonl"
    if not audit_path.exists():
        return []
    return [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_promote_prep_blocks_status_note_in_infra_mode(tmp_path: Path):
    """When promote_target_mode=infra, status-note promote targeting Doc-22 must be rejected."""
    run_id = "issue-mesh-20260327-301"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n\nstable\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(tmp_path, run_id)
    _write_infra_state(tmp_path, "infra")

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "NORMAL"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert resp.status_code == 409
        assert "DOC22_PROMOTE_BLOCKED_BY_INFRA_MODE" in resp.json()["detail"]


def test_promote_prep_blocks_current_layer_in_infra_mode(tmp_path: Path):
    """When promote_target_mode=infra, current-layer promote targeting Doc-22 must be rejected."""
    run_id = "issue-mesh-20260327-302"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n### 2.1 当前仍存活问题\n\n### 2.3 当前残余真实项\n\n### 4.5 后续优先级\n\n## 4.\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(tmp_path, run_id)
    _write_infra_state(tmp_path, "infra")

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/promote/current-layer",
            json={
                "run_id": run_id,
                "enabled": True,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "NORMAL"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert resp.status_code == 409
        assert "DOC22_PROMOTE_BLOCKED_BY_INFRA_MODE" in resp.json()["detail"]


def test_promote_prep_blocks_status_note_when_control_plane_state_missing(tmp_path: Path):
    run_id = "issue-mesh-20260327-312"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n\nstable\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(tmp_path, run_id)

    with _make_client(tmp_path, promote_target_mode=None) as client:
        resp = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "NORMAL"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 409
    assert "CONTROL_PLANE_STATE_MISSING" in resp.json()["detail"]
    events = _read_guard_audit(tmp_path)
    assert events[-1]["event"] == "doc22_promote_blocked"
    assert events[-1]["control_plane_state_reason"] == "CONTROL_PLANE_STATE_MISSING"


def test_promote_prep_blocks_status_note_when_control_plane_state_invalid(tmp_path: Path):
    run_id = "issue-mesh-20260327-313"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n\nstable\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(tmp_path, run_id)
    state_path = tmp_path / "automation" / "control_plane" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{not-json", encoding="utf-8")

    with _make_client(tmp_path, promote_target_mode=None) as client:
        resp = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "NORMAL"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )

    assert resp.status_code == 409
    assert "CONTROL_PLANE_STATE_INVALID" in resp.json()["detail"]
    events = _read_guard_audit(tmp_path)
    assert events[-1]["event"] == "doc22_promote_blocked"
    assert events[-1]["control_plane_state_reason"] == "CONTROL_PLANE_STATE_INVALID"


def test_promote_prep_allows_status_note_when_mode_is_doc22(tmp_path: Path):
    """When promote_target_mode=doc22, status-note promote should proceed normally."""
    run_id = "issue-mesh-20260327-303"
    doc22 = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc22.parent.mkdir(parents=True, exist_ok=True)
    doc22.write_text("# 22\n\n## 4.\n\nstable\n", encoding="utf-8")
    _seed_terminal_shards(tmp_path, run_id)
    _seed_shadow_bundle(tmp_path, run_id)
    _write_infra_state(tmp_path, "doc22")

    with _make_client(tmp_path) as client:
        resp = client.post(
            "/v1/promote/status-note",
            json={
                "run_id": run_id,
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "audit_context": {"public_runtime_status": "NORMAL"},
            },
            headers={"Authorization": "Bearer shadow-secret"},
        )
        assert resp.status_code == 200
