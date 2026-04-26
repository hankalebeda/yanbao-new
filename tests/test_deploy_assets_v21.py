from __future__ import annotations

from pathlib import Path
import pytest

_infra_missing = pytest.mark.xfail(
    reason="LiteLLM/ 和 docs/_temp/ 基础设施文件未部署到工作区", strict=False)


@_infra_missing
def test_kestra_docs_truth_converges_on_v21_canonical_plan():
    canonical_path = next(Path("docs/_temp").glob("Kestra_NewAPI_Writeback_*_v2.md"))
    canonical_text = canonical_path.read_text(encoding="utf-8")
    ubuntu_truth = Path("LiteLLM/ubuntu.txt").read_text(encoding="utf-8")

    assert "# Kestra + New API + 独立写回服务落地方案（v2.1）" in canonical_text
    assert "> 版本：v2.1" in canonical_text
    assert "唯一部署真值：`LiteLLM/ubuntu.txt`。" in canonical_text
    assert "2026-03-27 canonical deployment truth" in ubuntu_truth
    assert "kestra-internal-20260327" in ubuntu_truth
    assert "canonical readonly concurrency: `12`" in ubuntu_truth

    expected_header_lines = (
        "# 历史参考声明（重要）",
        "> 本文件仅作历史参考，不作为当前部署真值与执行基线。",
        "> 当前唯一部署真值：`LiteLLM/ubuntu.txt`。",
        "> 当前唯一执行基线：`docs/_temp/Kestra_NewAPI_Writeback_落地方案_v2.md`（v2.1）。",
    )
    for doc_path in sorted(Path("LiteLLM").glob("*.md")):
        header = "\n".join(doc_path.read_text(encoding="utf-8").splitlines()[:8])
        for expected_line in expected_header_lines:
            assert expected_line in header, f"missing historical-reference header in {doc_path}"


def test_check_stack_uses_kestra_flows_readiness_and_basic_auth():
    script = Path("automation/deploy/check-stack.ps1").read_text(encoding="utf-8")

    assert "/api/v1/flows" in script
    assert "/api/v1/flows/search" in script
    assert 'Get-EnvValue -Name "KESTRA_HTTP_PORT" -Default "18080"' in script
    assert "CheckExternalServices" in script
    assert "Authorization" in script
    assert "KESTRA_BASIC_AUTH_USERNAME" in script
    assert "KESTRA_BASIC_AUTH_PASSWORD" in script
    assert "NEW_API_BASE_URL" in script
    assert "MESH_RUNNER_BASE_URL" in script
    assert "PROMOTE_PREP_BASE_URL" in script
    assert "WRITEBACK_A_BASE_URL" in script
    assert "WRITEBACK_B_BASE_URL" in script


def test_deploy_readme_tracks_hybrid_topology_and_canonical_run_id():
    readme_text = Path("automation/deploy/README.md").read_text(encoding="utf-8")

    assert "Yanbao app + mesh_runner + promote_prep + writeback A + writeback B + loop_controller + real repo" in readme_text
    assert "issue-mesh-YYYYMMDD-NNN" in readme_text
    assert "`max_workers` (`12` for the canonical readonly run)" in readme_text
    assert "newapi-192.168.232.141-3000" in readme_text
    assert "Provision-NewApiReadonlyShards.ps1" in readme_text
    assert "newapi-192.168.232.141-3000-stable" in readme_text
    assert "CODEX_READONLY_PROVIDER_ALLOWLIST" in readme_text
    assert "check-stack.ps1 -CheckExternalServices" in readme_text
    assert "automatic triage" in readme_text
    assert "/v1/triage/writeback" in readme_text
    assert "WRITEBACK_REQUIRE_FENCING=false" in readme_text
    assert "WRITEBACK_REQUIRE_FENCING=true" in readme_text
    assert "app/**" in readme_text
    assert "tests/**" in readme_text
    assert "LiteLLM/**" not in readme_text
    assert "automation/**" not in readme_text
    assert "scripts/**" not in readme_text
    assert "runtime/issue_mesh/<run_id>/shard_XX/{task_spec.json,result.json}" in readme_text
    assert "docs/_temp/issue_mesh_shadow/<run_id>/{summary.md,bundle.json}" in readme_text
    assert "yanbao_master_loop" in readme_text
    assert "issue_mesh_code_fix_wave" in readme_text
    assert "shadow intent submission and shadow output generation through `promote_prep`" in readme_text
    assert "controller-only mode-derived preview/commit through writeback B" in readme_text
    assert "Controller-only promote flows use the formal `triage -> preview -> auto-commit` path." in readme_text


def test_compose_pins_kestra_image_tag():
    compose_text = Path("automation/deploy/docker-compose.kestra.yml").read_text(encoding="utf-8")

    assert "image: kestra/kestra:v1.3.2" in compose_text
    assert "image: kestra/kestra:latest" not in compose_text
    assert '${KESTRA_POSTGRES_PORT:-55433}:5432' in compose_text
    assert '${KESTRA_HTTP_PORT:-18080}:8080' in compose_text


def test_env_example_and_examples_capture_dual_writeback_and_bridge_ports():
    env_text = Path("automation/deploy/.env.example").read_text(encoding="utf-8")
    bridge_text = Path("automation/deploy/nginx.windows-bridge.conf").read_text(encoding="utf-8")
    audit_inputs = Path("automation/kestra/examples/audit_flow_inputs.json").read_text(encoding="utf-8")
    report_inputs = Path("automation/kestra/examples/report_flow_inputs.json").read_text(encoding="utf-8")
    status_inputs = Path("automation/kestra/examples/status_note_promote_inputs.json").read_text(encoding="utf-8")
    current_inputs = Path("automation/kestra/examples/current_layer_promote_inputs.json").read_text(encoding="utf-8")
    master_inputs = Path("automation/kestra/examples/master_loop_inputs.json").read_text(encoding="utf-8")

    assert "WRITEBACK_A_BASE_URL=http://host.docker.internal:18192" in env_text
    assert "MESH_RUNNER_BASE_URL=http://host.docker.internal:18193" in env_text
    assert "PROMOTE_PREP_BASE_URL=http://host.docker.internal:18194" in env_text
    assert "WRITEBACK_B_BASE_URL=http://host.docker.internal:18195" in env_text
    assert "ISSUE_MESH_READONLY_MAX_WORKERS=12" in env_text
    assert "ISSUE_MESH_MAX_WORKERS_CAP=12" in env_text
    assert "CODEX_AUDIT_GATEWAY_ONLY=false" in env_text
    assert "CODEX_CANONICAL_PROVIDER=newapi-192.168.232.141-3000-stable" in env_text
    assert "CODEX_READONLY_LANE=codex-readonly" in env_text
    assert "CODEX_STABLE_LANE=codex-stable" in env_text
    assert "CODEX_READONLY_PROVIDER_ALLOWLIST=newapi-192.168.232.141-3000-ro-a" in env_text
    assert "listen 18100;" in bridge_text
    assert "proxy_pass http://192.168.232.1:38001;" in bridge_text
    assert "proxy_set_header Host localhost:38001;" in bridge_text
    assert "listen 18192;" in bridge_text
    assert "proxy_pass http://192.168.232.1:8092;" in bridge_text
    assert "listen 18193;" in bridge_text
    assert "proxy_pass http://192.168.232.1:8093;" in bridge_text
    assert "listen 18194;" in bridge_text
    assert "proxy_pass http://192.168.232.1:8094;" in bridge_text
    assert "listen 18195;" in bridge_text
    assert "proxy_pass http://192.168.232.1:8095;" in bridge_text
    assert '"run_id": "issue-mesh-20260327-001"' in audit_inputs
    assert '"run_label": "issue-mesh-formal-monitor"' in audit_inputs
    assert '"max_workers": 12' in audit_inputs
    assert '"writeback_a_base_url": "http://host.docker.internal:18192"' in report_inputs
    assert '"promote_prep_base_url": "http://host.docker.internal:18194"' in report_inputs
    assert '"run_id": "report-writeback-20260327-001"' in report_inputs
    assert '"writeback_b_base_url": "http://host.docker.internal:18195"' in status_inputs
    assert '"do_rollback_after_commit": false' in status_inputs
    assert '"do_rollback_after_commit": false' in current_inputs
    assert '"autonomy_mode": "fix"' in master_inputs
    assert '"fix_goal": 10' in master_inputs
    assert '"await_timeout_seconds": 3600' in master_inputs
    assert '"round_id": "20260330-001"' in master_inputs
    assert '"loop_controller_base_url"' not in master_inputs


def test_start_and_watchdog_fail_closed_writeback_b_policy_from_control_plane_state():
    start_script = Path("automation/deploy/start-all-services.ps1").read_text(encoding="utf-8")
    watchdog_script = Path("automation/deploy/watchdog.ps1").read_text(encoding="utf-8")

    expected_infra_allow = "automation/control_plane/current_state.json,automation/control_plane/current_status.md"
    for script in (start_script, watchdog_script):
        assert 'Get-Env "CODEX_CANONICAL_PROVIDER" "newapi-192.168.232.141-3000-stable"' in script or 'Get-EnvVal "CODEX_CANONICAL_PROVIDER" "newapi-192.168.232.141-3000-stable"' in script
        assert "MISSING_CODEX_PROVIDER_HOME" in script
        assert "Provision-NewApiReadonlyShards.ps1" in script
        assert 'Name = "app"; Port = 38001' in script
        assert 'Module = "app.main:app"' in script
        assert 'WRITEBACK_ALLOW_PREFIXES" = "app/,tests/' in script or 'WRITEBACK_ALLOW_PREFIXES"   = "app/,tests/' in script
        assert 'WRITEBACK_REQUIRE_FENCING" = "false"' in script
        assert 'WRITEBACK_REQUIRE_FENCING" = "true"' in script
        assert "automation\\control_plane\\current_state.json" in script
        assert "CONTROL_PLANE_STATE_MISSING" in script
        assert "CONTROL_PLANE_STATE_INVALID" in script
        assert expected_infra_allow in script
        assert "app/,tests/,runtime/,LiteLLM/,docs/_temp/" in script
        assert "docs/core/" in script
        assert "docs/core/22_" in script
        assert "output/junit.xml" in script
        assert "app/governance/catalog_snapshot.json" in script
        assert "output/blind_spot_audit.json" in script
        assert "github/automation/continuous_audit/latest_run.json" in script
        assert "promote_target_mode" in script
        assert '"NO_PROXY"' in script
        assert '"no_proxy"' in script
        assert "127.0.0.1" in script
        assert "192.168.232.141" in script
        assert "192.168.232.1" in script


def test_bootstrap_tracks_app_plus_control_plane_services():
    bootstrap_text = Path("automation/deploy/bootstrap.ps1").read_text(encoding="utf-8")

    assert '@{ Name = "app"; Port = 38001 }' in bootstrap_text
    assert "Starting Windows app + control-plane services" in bootstrap_text
    assert "Services: 6 FastAPI instances on ports 38001,8092-8096" in bootstrap_text


def test_app_full_stack_startup_injects_autonomy_helper_env_and_watchdog_exists():
    start_text = Path("automation/deploy/start-all-services.ps1").read_text(encoding="utf-8")
    watchdog_text = Path("automation/deploy/watchdog.ps1").read_text(encoding="utf-8")
    readme_text = Path("automation/deploy/README.md").read_text(encoding="utf-8")

    for script in (start_text, watchdog_text):
        assert '"MESH_RUNNER_BASE_URL" = "http://127.0.0.1:8093"' in script
        assert '"MESH_RUNNER_AUTH_TOKEN" = $MESH_RUNNER_TOKEN' in script
        assert '"PROMOTE_PREP_BASE_URL" = "http://127.0.0.1:8094"' in script
        assert '"PROMOTE_PREP_AUTH_TOKEN" = $PROMOTE_PREP_TOKEN' in script
        assert '"WRITEBACK_A_BASE_URL" = "http://127.0.0.1:8092"' in script
        assert '"WRITEBACK_A_AUTH_TOKEN" = $WRITEBACK_A_TOKEN' in script
        assert '"WRITEBACK_B_BASE_URL" = "http://127.0.0.1:8095"' in script
        assert '"WRITEBACK_B_AUTH_TOKEN" = $WRITEBACK_B_TOKEN' in script
        assert '"NEW_API_BASE_URL" = $NEW_API_BASE_URL' in script
        assert '"NEW_API_TOKEN" = $NEW_API_TOKEN' in script
        assert '"CODEX_AUDIT_GATEWAY_ONLY" = $AUDIT_GATEWAY_ONLY' in script
        assert '"CODEX_READONLY_LANE" = $READONLY_LANE' in script
        assert '"CODEX_STABLE_LANE" = $STABLE_LANE' in script
        assert '"CODEX_READONLY_PROVIDER_ALLOWLIST" = $READONLY_PROVIDER_ALLOWLIST' in script
        assert '"INTERNAL_TOKEN" = $CONTROL_PLANE_TOKEN' in script
        assert '"INTERNAL_CRON_TOKEN" = $APP_INTERNAL_TOKEN' in script
        assert '"AUTONOMY_LOOP_ENABLED" = $AUTONOMY_LOOP_ENABLED' in script
        assert '"AUTONOMY_LOOP_MODE" = $AUTONOMY_LOOP_MODE' in script
        assert '"AUTONOMY_LOOP_FIX_GOAL" = $AUTONOMY_LOOP_FIX_GOAL' in script
        assert '"AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS" = $AUTONOMY_LOOP_AUDIT_INTERVAL' in script
        assert '"AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS" = $AUTONOMY_LOOP_MONITOR_INTERVAL' in script
        assert '"APP_BASE_URL" = "http://127.0.0.1:38001"' in script
        assert "NO_PROXY" in script
        assert "no_proxy" in script
        assert "runtime\\loop_controller\\runtime_lease.json" in script
        assert "start_$Name.cmd" in script or 'start_$Name.cmd' in script

    assert "Injects the same `MESH_RUNNER_*`, `PROMOTE_PREP_*`, `WRITEBACK_*`, and `NEW_API_*` contract into the app process" in readme_text
    assert "Mirrors `INTERNAL_CRON_TOKEN`, `AUTONOMY_LOOP_ENABLED`, `AUTONOMY_LOOP_MODE`" in readme_text
    assert "formal in-app autonomy runtime will fail closed on the first helper-service call" in readme_text


def test_provision_newapi_readonly_shards_wrapper_tracks_sharded_gateway_cli():
    script_text = Path("automation/deploy/Provision-NewApiReadonlyShards.ps1").read_text(encoding="utf-8")

    assert "--provision-gateway-only" in script_text
    assert "--write-sharded-gateway-provider-dirs" in script_text
    assert "--gateway-provider-name" in script_text
    assert '$stableProvider = "$GatewayProviderBaseName-stable"' in script_text
    assert "CODEX_READONLY_PROVIDER_ALLOWLIST" in script_text
    assert "output\\newapi_gateway_shards_latest.json" in script_text


@_infra_missing
def test_litellm_templates_track_sharded_gateway_truth():
    env_text = Path("LiteLLM/kestra_stack/.env.example").read_text(encoding="utf-8")
    compose_text = Path("LiteLLM/kestra_stack/docker-compose.yaml").read_text(encoding="utf-8")
    ubuntu_truth = Path("LiteLLM/ubuntu.txt").read_text(encoding="utf-8")

    assert "NEWAPI_TOKEN_NAME=codex-relay-xhigh" in env_text
    assert "NEWAPI_CANONICAL_PROVIDER=newapi-192.168.232.141-3000-stable" in env_text
    assert "NEWAPI_READONLY_PROVIDER_ALLOWLIST=newapi-192.168.232.141-3000-ro-a" in env_text
    assert "Provision-NewApiReadonlyShards.ps1" in env_text
    assert "Stable + readonly shard provisioning no longer happens in this Compose file." in compose_text
    assert "CODEX_CANONICAL_PROVIDER / CODEX_READONLY_PROVIDER_ALLOWLIST" in compose_text
    assert "canonical provider: `newapi-192.168.232.141-3000-stable`" in ubuntu_truth
    assert "token truth drift" in ubuntu_truth
    assert "codex-gpt54-xhigh" in ubuntu_truth


@_infra_missing
def test_plan_docs_converge_on_v21_canonical_truth():
    canonical_path = next(Path("docs/_temp").glob("Kestra_NewAPI_Writeback_*_v2.md"))
    final_path = next(Path("docs/_temp").glob("Kestra_NewAPI_Writeback_*_v3_final.md"))
    canonical_doc = canonical_path.read_text(encoding="utf-8")
    final_doc = final_path.read_text(encoding="utf-8")

    assert "版本：v2.1" in canonical_doc
    assert "定位：唯一执行基线" in canonical_doc
    assert "唯一部署真值：`LiteLLM/ubuntu.txt`。" in canonical_doc
    assert "# Kestra + New API + 独立写回服务落地方案 v4.1 (Final)" in final_doc
    assert "v4.1-final" in final_doc
    assert "Writeback A" in final_doc
    assert "Kestra Pause" in final_doc
    assert "deny `app/tests`" in final_doc

    historical_paths = sorted(Path("LiteLLM").glob("*.md"))
    historical_paths.append(final_path)

    for path in historical_paths:
        text = path.read_text(encoding="utf-8")
        assert "本文件仅作历史参考，不作为当前部署真值与执行基线。" in text, path.as_posix()
        assert "当前唯一部署真值：`LiteLLM/ubuntu.txt`。" in text, path.as_posix()
        assert "当前唯一执行基线：`docs/_temp/Kestra_NewAPI_Writeback_落地方案_v2.md`（v2.1）。" in text, path.as_posix()


def test_deploy_readme_describes_mode_derived_writeback_b_guardrails():
    readme_text = Path("automation/deploy/README.md").read_text(encoding="utf-8")

    assert "mode-derived preview/commit through writeback B" in readme_text
    assert "automation/control_plane/current_state.json" in readme_text
    assert "automation/control_plane/current_status.md" in readme_text
    assert "In `infra` mode, Writeback B must not point to:" in readme_text
    assert "Writeback B must set `WRITEBACK_REQUIRE_FENCING=true`" in readme_text
