from __future__ import annotations

import pytest
from automation.mesh_runner import manifest_builder

_infra_missing = pytest.mark.xfail(
    reason="LiteLLM/issue_mesh/ 目录未部署，manifest_builder 无法加载分片目录", strict=False)


def _minimal_required_catalog() -> list[dict[str, object]]:
    return [
        {
            "shard_id": f"shard_{index:02d}",
            "family_id": family_id,
            "goal": family_id,
            "role": role,
            "prompt_file": prompt_file,
            "prompt_template_id": template_id,
            "output_fields": ["issue_id", "suggested_action"],
            "ssot_refs": ssot_refs,
            "read_scope_docs": read_scope_docs,
        }
        for index, family_id, role, prompt_file, template_id, ssot_refs, read_scope_docs in [
            (1, "truth-lineage", "数据工程师", "truth_lineage.txt", "family_01_truth_lineage_v1", ["04"], ["22_*v7*.md", "25_*.md", "04_*.md"]),
            (2, "runtime-anchor", "测试与质量", "runtime_anchor.txt", "family_02_runtime_anchor_v1", ["03", "04"], ["22_*v7*.md", "25_*.md", "03_*.md", "04_*.md"]),
            (3, "fr07-rebuild", "数据工程师", "fr07_rebuild.txt", "family_03_fr07_rebuild_v1", ["01", "03", "04"], ["22_*v7*.md", "25_*.md", "01_*.md", "03_*.md", "04_*.md"]),
            (4, "fr06-failure-semantics", "研报生成工程师", "fr06_failure_semantics.txt", "family_04_fr06_failure_semantics_v1", ["01", "03", "05"], ["22_*v7*.md", "25_*.md", "01_*.md", "03_*.md", "05_*.md"]),
            (5, "payment-auth-governance", "商业与鉴权", "payment_auth_governance.txt", "family_05_payment_auth_governance_v1", ["01", "02", "05"], ["22_*v7*.md", "25_*.md", "01_*.md", "02_*.md", "05_*.md"]),
            (6, "internal-contracts", "测试与质量", "internal_contracts.txt", "family_06_internal_contracts_v1", ["03", "05"], ["22_*v7*.md", "25_*.md", "03_*.md", "05_*.md"]),
            (7, "shared-artifacts", "测试与质量", "shared_artifacts.txt", "family_07_shared_artifacts_v1", ["05"], ["22_*v7*.md", "25_*.md", "05_*.md"]),
            (8, "issue-registry", "测试与质量", "issue_registry.txt", "family_08_issue_registry_v1", [], ["22_*v7*.md", "25_*.md"]),
            (9, "repo-governance", "测试与质量", "repo_governance.txt", "family_09_repo_governance_v1", [], ["22_*v7*.md", "25_*.md"]),
            (10, "external-integration", "商业与鉴权", "external_integration.txt", "family_10_external_integration_v1", ["05"], ["22_*v7*.md", "25_*.md", "05_*.md"]),
            (11, "display-bridge", "前端与体验", "display_bridge.txt", "family_11_display_bridge_v1", ["02", "05"], ["22_*v7*.md", "25_*.md", "02_*.md", "05_*.md"]),
            (12, "execution-order", "测试与质量", "execution_order.txt", "family_12_execution_order_v1", [], ["22_*v7*.md", "25_*.md"]),
        ]
    ]


@_infra_missing
def test_build_readonly_manifest_always_materializes_twelve_shards(monkeypatch):
    monkeypatch.setattr(manifest_builder, "_load_family_catalog", lambda: _minimal_required_catalog())
    monkeypatch.setattr(manifest_builder, "_resolve_doc", lambda pattern: f"docs/core/{pattern}")
    monkeypatch.setattr(manifest_builder, "_runtime_issue_mesh_state", lambda: {"runs": [], "promote_prep_intents": []})

    manifest = manifest_builder.build_readonly_manifest(
        provider_allowlist=["newapi-192.168.232.141-3000"],
        max_workers=12,
        benchmark_label="issue-mesh-readonly",
    )

    assert manifest.max_workers == 12
    assert len(manifest.tasks) == 12
    assert manifest.tasks[0].task_id == "truth-lineage"
    assert manifest.tasks[-1].task_id == "execution-order"
    assert all(task.timeout_seconds == manifest_builder.DEFAULT_SHARD_TIMEOUT_SECONDS for task in manifest.tasks)
    assert all(task.output_mode == "json" for task in manifest.tasks)
    assert all("app" in task.read_scope for task in manifest.tasks)
    assert all("tests" in task.read_scope for task in manifest.tasks)
    assert all("automation" in task.read_scope for task in manifest.tasks)


@_infra_missing
def test_build_readonly_manifest_includes_audit_context_in_prompt(monkeypatch):
    monkeypatch.setattr(manifest_builder, "_load_family_catalog", lambda: _minimal_required_catalog())
    monkeypatch.setattr(manifest_builder, "_resolve_doc", lambda pattern: f"docs/core/{pattern}")
    monkeypatch.setattr(
        manifest_builder,
        "_runtime_issue_mesh_state",
        lambda: {"latest_runs": [{"run_id": "issue-mesh-20260327-001", "status": "running"}]},
    )

    manifest = manifest_builder.build_readonly_manifest(
        provider_allowlist=["newapi-192.168.232.141-3000"],
        audit_context={"runtime_gates": {"status": "blocked"}, "public_runtime_status": "degraded"},
        extra_read_scope=["runtime/issue_mesh/issue-mesh-20260327-001/audit_context.json"],
    )

    assert "Runtime Audit Context" in manifest.tasks[0].prompt
    assert "Runtime Issue Mesh State" in manifest.tasks[0].prompt
    assert "Do not edit runtime/issue_mesh/<run_id>/<shard_id>/result.json directly." in manifest.tasks[0].prompt
    assert "mesh_runner persists shard result.json" in manifest.tasks[0].prompt
    assert "Write only runtime/issue_mesh/<run_id>/<shard_id>/result.json." not in manifest.tasks[0].prompt
    assert "runtime/issue_mesh/issue-mesh-20260327-001/audit_context.json" in manifest.tasks[0].read_scope
    assert "runtime/issue_mesh" in manifest.tasks[0].read_scope
    assert "docs/_temp/issue_mesh_shadow" in manifest.tasks[0].read_scope


@_infra_missing
def test_build_readonly_manifest_exposes_scope_and_strategy_in_prompt(monkeypatch):
    monkeypatch.setattr(manifest_builder, "_load_family_catalog", lambda: _minimal_required_catalog())
    monkeypatch.setattr(manifest_builder, "_resolve_doc", lambda pattern: f"docs/core/{pattern}")
    monkeypatch.setattr(manifest_builder, "_runtime_issue_mesh_state", lambda: {})

    manifest = manifest_builder.build_readonly_manifest(
        provider_allowlist=["newapi-192.168.232.141-3000"],
        audit_scope="priority-only",
        shard_strategy="family-view-ssot-v2",
    )

    assert "audit_scope: `priority-only`" in manifest.tasks[0].prompt
    assert "shard_strategy: `family-view-ssot-v2`" in manifest.tasks[0].prompt
    assert "prompt_template_id: `family_01_truth_lineage_v1`" in manifest.tasks[0].prompt
    assert "output_fields: `['issue_id', 'suggested_action']`" in manifest.tasks[0].prompt


@_infra_missing
def test_build_readonly_manifest_routes_shards_to_two_home_ring(monkeypatch):
    monkeypatch.setattr(manifest_builder, "_load_family_catalog", lambda: _minimal_required_catalog())
    monkeypatch.setattr(manifest_builder, "_resolve_doc", lambda pattern: f"docs/core/{pattern}")
    monkeypatch.setattr(manifest_builder, "_runtime_issue_mesh_state", lambda: {})

    manifest = manifest_builder.build_readonly_manifest(
        provider_allowlist=[
            "newapi-192.168.232.141-3000-stable",
            "newapi-192.168.232.141-3000-ro-a",
            "newapi-192.168.232.141-3000-ro-b",
            "newapi-192.168.232.141-3000-ro-c",
            "newapi-192.168.232.141-3000-ro-d",
        ],
        max_workers=12,
    )

    assert manifest.tasks[0].provider_allowlist == [
        "newapi-192.168.232.141-3000-ro-a",
        "newapi-192.168.232.141-3000-ro-b",
    ]
    assert manifest.tasks[1].provider_allowlist == [
        "newapi-192.168.232.141-3000-ro-b",
        "newapi-192.168.232.141-3000-ro-c",
    ]
    assert manifest.tasks[3].provider_allowlist == [
        "newapi-192.168.232.141-3000-ro-d",
        "newapi-192.168.232.141-3000-ro-a",
    ]
    assert "route_lane: `codex-ro-a`" in manifest.tasks[0].prompt
    assert "route_fallback_provider: `newapi-192.168.232.141-3000-ro-b`" in manifest.tasks[0].prompt
