from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def test_issue_mesh_flow_and_docs_are_present():
    health_flow_text = (_ROOT / "automation/kestra/flows/yanbao_health_checks.yml").read_text(encoding="utf-8")
    report_flow_text = (_ROOT / "automation/kestra/flows/yanbao_report_writeback_orchestration.yml").read_text(
        encoding="utf-8"
    )
    audit_flow_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_audit.yml").read_text(encoding="utf-8")
    code_fix_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_code_fix_wave.yml").read_text(encoding="utf-8")
    status_note_flow_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_status_note_promote.yml").read_text(
        encoding="utf-8"
    )
    current_layer_flow_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_current_layer_promote.yml").read_text(
        encoding="utf-8"
    )
    master_loop_text = (_ROOT / "automation/kestra/flows/yanbao_master_loop.yml").read_text(encoding="utf-8")
    readme_text = (_ROOT / "automation/kestra/README.md").read_text(encoding="utf-8")

    assert "id: yanbao_health_checks" in health_flow_text
    assert "mesh_runner_base_url" in health_flow_text
    assert "promote_prep_base_url" in health_flow_text
    assert "writeback_a_base_url" in health_flow_text
    assert "writeback_b_base_url" in health_flow_text
    assert "autonomy_loop_status" in health_flow_text
    assert "mesh_runner_health" in health_flow_text
    assert "promote_prep_health" in health_flow_text
    assert "writeback_a_health" in health_flow_text
    assert "writeback_b_health" in health_flow_text
    assert "loop_controller_base_url" not in health_flow_text
    assert "loop_controller_health" not in health_flow_text

    assert "id: yanbao_report_writeback_orchestration" in report_flow_text
    assert "/v1/triage/writeback" in report_flow_text
    assert "triage_decision" in report_flow_text
    assert 'condition: "{{ outputs.triage_decision.body | jq(\'.auto_commit\') | first }}"' in report_flow_text
    assert "run_id" in report_flow_text
    assert "promote_prep_base_url" in report_flow_text
    assert "commit_if_preview_clean" in report_flow_text
    assert "manual_approval_pause" not in report_flow_text
    assert 'condition: "{{ not (outputs.writeback_preview.body | jq(\'.conflict\') | first) }}"' in report_flow_text
    assert "separate from the issue-mesh repair closure" in report_flow_text

    assert "id: yanbao_issue_mesh_audit" in audit_flow_text
    assert "run_id" in audit_flow_text
    assert "runtime_gates" in audit_flow_text
    assert "audit_context" in audit_flow_text
    assert "start_mesh_run" in audit_flow_text
    assert "submit_shadow_intent" in audit_flow_text
    assert "poll_shadow_intent" in audit_flow_text
    assert "/v1/runs/{{ inputs.run_id }}" in audit_flow_text
    assert '"audit_context": {{ outputs.audit_context.body | jq(\'.data\') | first | json }}' in audit_flow_text
    assert '"summary_markdown": {{ outputs.poll_mesh_run.body | jq(\'.summary_markdown\') | first | json }}' in audit_flow_text
    assert "outputs.start_mesh_run.body.run_id" not in audit_flow_text
    assert "readTimeout: PT40M" in audit_flow_text
    assert "wait_for_completion=true&wait_timeout_seconds=900" in audit_flow_text
    assert "promote_prep" in audit_flow_text
    assert "audit_scope" in audit_flow_text
    assert "shard_strategy" in audit_flow_text
    assert "control_state_snapshot" in audit_flow_text
    assert '  - id: run_id\n    type: STRING\n    required: true' in audit_flow_text
    assert '  - id: max_workers\n    type: INT\n    defaults: 12' in audit_flow_text
    assert "issue-mesh-{{ execution.id }}" not in audit_flow_text
    assert "host.docker.internal:18193" in audit_flow_text
    assert "host.docker.internal:18194" in audit_flow_text
    assert "downstream_code_fix_source_run_id" in audit_flow_text
    assert "Formal readonly monitor/analyze stage" in audit_flow_text

    assert "id: yanbao_issue_mesh_code_fix_wave" in code_fix_text
    assert "Formal fix -> verify -> writeback stage" in code_fix_text

    assert "id: yanbao_issue_mesh_status_note_promote" in status_note_flow_text
    assert "/v1/promote/status-note" in status_note_flow_text
    assert "writeback_b_base_url" in status_note_flow_text
    assert 'condition: "{{ inputs.enabled }}"' not in status_note_flow_text
    assert "status_note_disabled" not in status_note_flow_text
    assert "allowFailed: true" in status_note_flow_text
    assert 'condition: "{{ outputs.prepare_status_note.code == 200 }}"' in status_note_flow_text
    assert 'condition: "{{ not (outputs.prepare_status_note.body | jq(\'.skip_commit\') | first) }}"' in status_note_flow_text
    assert "/v1/triage" in status_note_flow_text
    assert "triage_decision" in status_note_flow_text
    assert 'condition: "{{ outputs.triage_decision.body | jq(\'.auto_commit\') | first }}"' in status_note_flow_text
    assert "writeback_preview" in status_note_flow_text
    assert "commit_if_triage_allow" in status_note_flow_text
    assert "skip_status_note_prepare" in status_note_flow_text
    assert "manual_approval_pause" not in status_note_flow_text
    assert "/v1/rollback" in status_note_flow_text
    assert "/v1/promote/rollback-acceptance" in status_note_flow_text
    assert "expected_shadow_snapshot" in status_note_flow_text
    assert "skip_status_note_commit" in status_note_flow_text
    assert "skip_status_note_commit_after_triage" in status_note_flow_text
    assert '  - id: lease_id\n    type: STRING\n    required: true' in status_note_flow_text
    assert '  - id: fencing_token\n    type: INT\n    required: true' in status_note_flow_text
    assert '"lease_id": "{{ inputs.lease_id }}"' in status_note_flow_text
    assert '"fencing_token": {{ inputs.fencing_token }}' in status_note_flow_text

    assert "id: yanbao_issue_mesh_current_layer_promote" in current_layer_flow_text
    assert "/v1/promote/current-layer" in current_layer_flow_text
    assert 'condition: "{{ inputs.enabled }}"' not in current_layer_flow_text
    assert '"enabled": true' in current_layer_flow_text
    assert "current_layer_disabled" not in current_layer_flow_text
    assert "allowFailed: true" in current_layer_flow_text
    assert 'condition: "{{ outputs.prepare_current_layer.code == 200 }}"' in current_layer_flow_text
    assert "writeback_preview" in current_layer_flow_text
    assert "writeback_commit" in current_layer_flow_text
    assert "triage_decision" in current_layer_flow_text
    assert 'condition: "{{ outputs.triage_decision.body | jq(\'.auto_commit\') | first }}"' in current_layer_flow_text
    assert "commit_if_triage_allow" in current_layer_flow_text
    assert "manual_approval_pause" not in current_layer_flow_text
    assert "/v1/triage" in current_layer_flow_text
    assert "/v1/rollback" in current_layer_flow_text
    assert "/v1/promote/rollback-acceptance" in current_layer_flow_text
    assert 'condition: "{{ not (outputs.prepare_current_layer.body | jq(\'.skip_commit\') | first) }}"' in current_layer_flow_text
    assert "skip_current_layer_commit" in current_layer_flow_text
    assert "skip_current_layer_commit_after_triage" in current_layer_flow_text
    assert "skip_current_layer_prepare" in current_layer_flow_text
    assert '  - id: lease_id\n    type: STRING\n    required: true' in current_layer_flow_text
    assert '  - id: fencing_token\n    type: INT\n    required: true' in current_layer_flow_text
    assert '"lease_id": "{{ inputs.lease_id }}"' in current_layer_flow_text
    assert '"fencing_token": {{ inputs.fencing_token }}' in current_layer_flow_text

    assert "id: yanbao_master_loop" in master_loop_text
    assert "POST /api/v1/internal/autonomy/loop/start" in readme_text
    assert "GET /api/v1/internal/autonomy/loop/await-round" in readme_text

    assert "flows/yanbao_health_checks.yml" in readme_text
    assert "flows/yanbao_report_writeback_orchestration.yml" in readme_text
    assert "flows/yanbao_issue_mesh_audit.yml" in readme_text
    assert "flows/yanbao_issue_mesh_code_fix_wave.yml" in readme_text
    assert "flows/yanbao_issue_mesh_status_note_promote.yml" in readme_text
    assert "flows/yanbao_issue_mesh_current_layer_promote.yml" in readme_text
    assert "flows/yanbao_master_loop.yml" in readme_text
    assert "docs/_temp/issue_mesh_shadow/<run_id>/{summary.md,bundle.json}" in readme_text
    assert "writeback_b_base_url" in readme_text
    assert "issue-mesh-YYYYMMDD-NNN" in readme_text
    assert "set to `12`" in readme_text
    assert "Passes the app-provided `audit_context` through to `mesh-runner`" in readme_text
    assert "AI risk analysis" in readme_text
    assert "/v1/triage" in readme_text
    assert "triage -> preview -> auto-commit" in readme_text
    assert "rollback acceptance" in readme_text
    assert "lease_id + fencing_token" in readme_text
    assert "yanbao_health_checks" in readme_text
    assert "yanbao_report_writeback_orchestration" in readme_text
    assert "yanbao_issue_mesh_code_fix_wave" in readme_text
    assert "yanbao_master_loop" in readme_text
    assert "Formal issue-mesh closure orchestrator" in readme_text
    assert "Kestra only uses `/autonomy/loop/start`, `/autonomy/loop/await-round`, and `/autonomy/loop`" in readme_text
    assert "Promote flows no longer rely on a manual `enabled=false` reservation switch" in readme_text


def test_master_loop_flow_contract_alignment():
    """Verify master_loop.yml delegates the round lifecycle to the internal autonomy runtime."""
    master_loop_text = (_ROOT / "automation/kestra/flows/yanbao_master_loop.yml").read_text(encoding="utf-8")

    assert "flowId: yanbao_health_checks" in master_loop_text
    assert "/api/v1/internal/runtime/gates" in master_loop_text
    assert "/api/v1/internal/audit/context" in master_loop_text
    assert "/api/v1/internal/autonomy/loop" in master_loop_text
    assert "/api/v1/internal/autonomy/loop/start" in master_loop_text
    assert "/api/v1/internal/autonomy/loop/await-round" in master_loop_text
    assert "/v1/round-complete" not in master_loop_text
    assert "loop_controller_base_url" not in master_loop_text
    assert "loop_controller_token" not in master_loop_text
    assert '"force_new_round": true' in master_loop_text
    assert "start_autonomy_round" in master_loop_text
    assert "await_autonomy_round" in master_loop_text
    assert "run_readonly_issue_mesh" not in master_loop_text
    assert "run_code_fix_wave" not in master_loop_text
    assert "run_status_note_promote" not in master_loop_text
    assert "run_current_layer_promote" not in master_loop_text


def test_health_checks_flow_template_references():
    """Verify health_checks.yml uses correct outputs.* prefix for all task references."""
    health_text = (_ROOT / "automation/kestra/flows/yanbao_health_checks.yml").read_text(encoding="utf-8")

    assert "outputs.new_api_models.body" in health_text, (
        "new_api_responses_smoke must reference outputs.new_api_models (not bare new_api_models)"
    )

    bare_task_refs = re.findall(r"\{\{[^}]*(?<!\.)new_api_models\.body", health_text)
    bad_refs = [ref for ref in bare_task_refs if "outputs.new_api_models" not in ref]
    assert not bad_refs, f"Found bare task references without outputs. prefix: {bad_refs}"

    gate_section_start = health_text.find("health_gate_result")
    assert gate_section_start != -1
    gate_section = health_text[gate_section_start:]
    assert "outputs.autonomy_loop_status.code" in gate_section
    assert "outputs.loop_controller_health.code" not in gate_section
    for svc in ["mesh_runner_health", "promote_prep_health", "writeback_a_health", "writeback_b_health"]:
        assert f"outputs.{svc}.code" in gate_section, f"health_gate_result must use outputs.{svc}.code"


def test_code_fix_wave_flow_structure():
    """Verify code_fix_wave.yml has batch-commit (not single-patch) and triage integration."""
    code_fix_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_code_fix_wave.yml").read_text(encoding="utf-8")

    assert "id: yanbao_issue_mesh_code_fix_wave" in code_fix_text
    assert "/v1/batch-commit" in code_fix_text, "Must use batch-commit endpoint"
    assert "/v1/triage/synthesize-patches" in code_fix_text
    assert "/v1/triage/writeback" in code_fix_text
    assert "triage_record_ids" in code_fix_text
    assert "idempotency" in code_fix_text.lower() or "request_id" in code_fix_text
    assert "timeout: PT5M" in code_fix_text
    assert "readTimeout: PT5M" in code_fix_text
    assert "readIdleTimeout: PT5M" in code_fix_text
    assert "connectTimeout: PT1M" in code_fix_text


def test_promote_prep_ai_flow_tasks_use_five_minute_timeouts():
    report_flow_text = (_ROOT / "automation/kestra/flows/yanbao_report_writeback_orchestration.yml").read_text(
        encoding="utf-8"
    )
    status_note_flow_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_status_note_promote.yml").read_text(
        encoding="utf-8"
    )
    current_layer_flow_text = (_ROOT / "automation/kestra/flows/yanbao_issue_mesh_current_layer_promote.yml").read_text(
        encoding="utf-8"
    )

    assert "/v1/triage/writeback" in report_flow_text
    assert "timeout: PT5M" in report_flow_text
    assert "readTimeout: PT5M" in report_flow_text
    assert "readIdleTimeout: PT5M" in report_flow_text
    assert "connectTimeout: PT1M" in report_flow_text
    assert "/v1/triage" in status_note_flow_text
    assert "timeout: PT5M" in status_note_flow_text
    assert "readTimeout: PT5M" in status_note_flow_text
    assert "readIdleTimeout: PT5M" in status_note_flow_text
    assert "connectTimeout: PT1M" in status_note_flow_text
    assert "/v1/triage" in current_layer_flow_text
    assert "timeout: PT5M" in current_layer_flow_text
    assert "readTimeout: PT5M" in current_layer_flow_text
    assert "readIdleTimeout: PT5M" in current_layer_flow_text
    assert "connectTimeout: PT1M" in current_layer_flow_text


def test_master_loop_flow_has_all_required_inputs():
    """Verify master_loop.yml declares all required inputs for the formal closure."""
    master_loop_text = (_ROOT / "automation/kestra/flows/yanbao_master_loop.yml").read_text(encoding="utf-8")

    required_inputs = [
        "app_base_url",
        "internal_token",
        "new_api_base_url",
        "new_api_token",
        "mesh_runner_base_url",
        "promote_prep_base_url",
        "writeback_a_base_url",
        "writeback_b_base_url",
        "autonomy_mode",
        "fix_goal",
        "await_timeout_seconds",
        "round_id",
    ]
    for inp in required_inputs:
        assert f"id: {inp}" in master_loop_text, f"Missing required input: {inp}"
