from __future__ import annotations

import pytest
from scripts import queue22_recovery_rearm_slots

_infra_missing = pytest.mark.xfail(
    reason="docs/_temp/ 任务文件未部署到工作区", strict=False)


@_infra_missing
def test_build_manifest_contains_five_rebaseline_slots_by_default():
    root = queue22_recovery_rearm_slots.repo_root()

    manifest = queue22_recovery_rearm_slots.build_manifest(root)
    tasks_by_id = {task.task_id: task for task in manifest.tasks}

    assert manifest.execution_mode == "mesh"
    assert manifest.max_workers == 6
    assert len(manifest.tasks) == 5
    assert [task.task_id for task in manifest.tasks] == [
        "slot06-artifact-delta-auditor",
        "slot07-current-layer-miner-a",
        "slot08-current-layer-miner-b",
        "slot09-task-audit-miner",
        "slot13-writeback-drafter",
    ]
    assert all(task.task_kind == "analysis" for task in manifest.tasks)
    assert all(task.write_scope == [] for task in manifest.tasks)
    assert all(task.working_root == str(root) for task in manifest.tasks)
    assert "docs/_temp/problem/README.md" in tasks_by_id["slot06-artifact-delta-auditor"].read_scope
    assert "docs/_temp/problem/ANALYSIS_MASTER_LOCK__owner-controller02__status-active.md" in tasks_by_id["slot07-current-layer-miner-a"].read_scope
    assert "docs/_temp/problem/SLOT_OCCUPANCY.md" in tasks_by_id["slot08-current-layer-miner-b"].read_scope


@_infra_missing
def test_full_profile_contains_ten_readonly_slots():
    root = queue22_recovery_rearm_slots.repo_root()

    manifest = queue22_recovery_rearm_slots.build_manifest(root, profile="full")
    payload = queue22_recovery_rearm_slots.manifest_payload(manifest)

    assert payload["execution_mode"] == "mesh"
    assert payload["max_workers"] == 6
    assert len(payload["tasks"]) == 10
    assert payload["tasks"][0]["task_id"] == "slot04-runtime-readback-auditor"
    assert payload["tasks"][-1]["task_id"] == "slot13-writeback-drafter"


@_infra_missing
def test_support_profile_contains_two_runtime_support_slots():
    root = queue22_recovery_rearm_slots.repo_root()

    manifest = queue22_recovery_rearm_slots.build_manifest(root, profile="support")

    assert manifest.max_workers == 6
    assert [task.task_id for task in manifest.tasks] == [
        "slot04-runtime-readback-auditor",
        "slot05-claim-resource-guard",
    ]
    assert all("docs/_temp/problem/README.md" in task.read_scope for task in manifest.tasks)
    assert all("docs/_temp/problem/SLOT_OCCUPANCY.md" in task.read_scope for task in manifest.tasks)


@_infra_missing
def test_round2_prompt_snapshots_and_doc_paths_are_authoritative():
    root = queue22_recovery_rearm_slots.repo_root()
    prompt_dir = root / queue22_recovery_rearm_slots.PROMPT_DIR

    assert queue22_recovery_rearm_slots.DOC_22 == "docs/core/22_全量功能进度总表_v7_精审.md"
    assert queue22_recovery_rearm_slots.DOC_25 == "docs/core/25_系统问题分析角度清单.md"
    assert (root / queue22_recovery_rearm_slots.DOC_22).exists()
    assert (root / queue22_recovery_rearm_slots.DOC_25).exists()

    expected_prompt_files = {
        "claim_resource_guard_20260326c_r2.txt",
        "miner_current_layer_20260326c_r2.txt",
        "review_precheck_20260326c_r2.txt",
    }

    configured_prompt_files = {
        item["prompt_file"]
        for item in (
            queue22_recovery_rearm_slots.SUPPORT_SLOT_CONFIGS
            + queue22_recovery_rearm_slots.REBASELINE_SLOT_CONFIGS
            + queue22_recovery_rearm_slots.FULL_SLOT_CONFIGS
        )
    }

    assert expected_prompt_files.issubset(configured_prompt_files)
    assert all((prompt_dir / prompt_file).exists() for prompt_file in expected_prompt_files)

    artifact_prompt = (prompt_dir / "artifact_delta_auditor_20260326c_r2.txt").read_text(encoding="utf-8")
    miner_prompt = (prompt_dir / "miner_current_layer_20260326c_r2.txt").read_text(encoding="utf-8")
    controller_prompt = (prompt_dir / "controller_queue22_20260326c_controller13_r2.txt").read_text(encoding="utf-8")

    assert "1180 vs 1168" in artifact_prompt
    assert "1183" not in artifact_prompt
    assert "warn_features=69" in artifact_prompt
    assert "collect-only=1180" in miner_prompt
    assert "registry_stats.warn_features=69" in miner_prompt
    assert "collect-only=1180" in controller_prompt
    assert "registry_stats.warn_features=69" in controller_prompt
