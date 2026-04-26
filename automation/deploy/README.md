# Kestra Deployment Assets

This directory provides deployment assets for the Kestra control plane that orchestrates:

- app API calls (health + report generation)
- remote New API checks (`http://192.168.232.141:3000`)
- independent writeback preview/commit APIs with automatic triage for controller-only promote
- readonly issue-mesh execution through `mesh-runner`
- shadow intent submission and shadow output generation through `promote_prep`
- controller-only mode-derived preview/commit through writeback B

It does not directly write business databases.

## Recommended Topology

Recommended production topology is hybrid deployment:

- `Ubuntu`: `Kestra + PostgreSQL + New API`
- `Windows / D:\yanbao`: `Yanbao app + mesh_runner + promote_prep + writeback A + writeback B + loop_controller + real repo`

Rationale:

- Kestra and New API are long-running control-plane services and fit a stable Docker-capable Ubuntu host better.
- The writeback service must stay close to the real `D:\yanbao` repository so its `read / preview / commit / rollback` semantics continue to apply to the single source of truth.
- Do not treat the writeback service as a generic remote file mutator against a copied repository.

## Canonical Runtime Inputs

Use this exact logical set across flows, scripts, and runbooks:

- `app_base_url`
- `internal_token`
- `new_api_base_url`
- `new_api_token`
- `writeback_a_base_url`
- `writeback_a_token`
- `writeback_b_base_url`
- `writeback_b_token`
- `mesh_runner_base_url`
- `mesh_runner_token`
- `promote_prep_base_url`
- `promote_prep_token`

## Files

- `docker-compose.kestra.yml`: Kestra + PostgreSQL stack for the Ubuntu control plane.
- `.env.example`: runtime variables template.
- `start-kestra.ps1`: starts the stack.
- `start-all-services.ps1`: starts the Windows-hosted `app (38001)` plus `writeback A/B`, `mesh_runner`, `promote_prep`, and `loop_controller`.
- `watchdog.ps1`: monitors and restarts the Windows-hosted `app + control-plane` services.
- `check-stack.ps1`: checks stack status and Kestra readiness through `GET /api/v1/flows`.
- `check-stack.ps1 -CheckExternalServices`: additionally checks New API, app, mesh_runner, promote_prep, writeback A, writeback B, and loop_controller health endpoints from the configured env URLs.
- `nginx.windows-bridge.conf`: Ubuntu-side HTTP bridge config for `18100/18192/18193/18194/18195/18196` back to the Windows host on `192.168.232.1`.
- Default HTTP port is `18080`, which matches the currently validated Ubuntu deployment.

## Quick Start

1. Copy env template:

   - `Copy-Item automation/deploy/.env.example automation/deploy/.env`

2. Fill `.env` secrets:

   - `KESTRA_BASIC_AUTH_USERNAME`
   - `KESTRA_BASIC_AUTH_PASSWORD`
   - `INTERNAL_TOKEN`
   - `NEW_API_TOKEN`
   - `WRITEBACK_A_TOKEN`
   - `WRITEBACK_B_TOKEN`
   - `MESH_RUNNER_TOKEN`
   - `PROMOTE_PREP_TOKEN`
   - `LOOP_CONTROLLER_TOKEN`

3. Start the Windows-hosted app + control plane with one command:

   - `powershell -ExecutionPolicy Bypass -File automation/deploy/start-all-services.ps1`
   - Starts `app (38001)`, `writeback A (8092)`, `mesh_runner (8093)`, `promote_prep (8094)`, `writeback B (8095)`, and `loop_controller (8096)`.
   - Injects `NO_PROXY=127.0.0.1,localhost,::1,192.168.232.141,192.168.232.1` into child processes so localhost and bridge traffic never goes through desktop HTTP proxies.
   - Injects the same `MESH_RUNNER_*`, `PROMOTE_PREP_*`, `WRITEBACK_*`, and `NEW_API_*` contract into the app process so `/api/v1/internal/autonomy/loop/*` can drive the formal helper services without extra manual env stitching.
   - Mirrors `INTERNAL_CRON_TOKEN`, `AUTONOMY_LOOP_ENABLED`, `AUTONOMY_LOOP_MODE`, `AUTONOMY_LOOP_FIX_GOAL`, `AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS`, and `AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS` into the app process so watchdog restarts preserve the same in-app control-plane contract.

4. Or start writeback A on the Windows machine that holds the real `D:\yanbao` repository:

   - `set WRITEBACK_AUTH_TOKEN=<same as WRITEBACK_A_TOKEN>`
   - `set WRITEBACK_REQUIRE_TRIAGE=true`
   - `set WRITEBACK_REQUIRE_FENCING=false`
   - `set WRITEBACK_ALLOW_PREFIXES=app/,tests/`
   - `set WRITEBACK_DENY_PREFIXES=runtime/`
   - `set WRITEBACK_DENY_PATHS=docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md,output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json`
   - `python -m uvicorn automation.writeback_service.app:app --host 0.0.0.0 --port 8092`

5. Start writeback B on the same Windows machine with mode-derived policy:

   - `set WRITEBACK_AUTH_TOKEN=<same as WRITEBACK_B_TOKEN>`
   - `set WRITEBACK_REQUIRE_TRIAGE=true`
   - `set WRITEBACK_REQUIRE_FENCING=true`
   - `infra` mode (`automation/control_plane/current_state.json` missing/invalid or `promote_target_mode=infra`):
   - `set WRITEBACK_ALLOW_PREFIXES=automation/control_plane/current_state.json,automation/control_plane/current_status.md`
   - `set WRITEBACK_DENY_PREFIXES=app/,tests/,runtime/,LiteLLM/,docs/_temp/,docs/core/`
   - `set WRITEBACK_DENY_PATHS=docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md,output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json`
   - `doc22` mode (`promote_target_mode=doc22`):
   - `set WRITEBACK_ALLOW_PREFIXES=docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md`
   - `set WRITEBACK_DENY_PREFIXES=app/,tests/,runtime/,LiteLLM/,docs/_temp/`
   - `set WRITEBACK_DENY_PATHS=output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json`
   - `set WRITEBACK_AUDIT_DIR=D:\yanbao\automation\writeback_service\.audit_writeback_b`
   - `python -m uvicorn automation.writeback_service.app:app --host 0.0.0.0 --port 8095`

6. Materialize the stable + readonly gateway provider homes and lane-scoped tokens before the readonly stack starts:

   - `powershell -ExecutionPolicy Bypass -File automation/deploy/Provision-NewApiReadonlyShards.ps1 -Username <newapi-admin> -Password <newapi-password>`
   - The script writes `output/newapi_gateway_shards_latest.json` and creates `ai-api/codex/newapi-192.168.232.141-3000-stable` plus `ro-a..ro-d`.

7. Start the readonly issue-mesh helpers on the Windows machine that holds the real `D:\yanbao` repository:

   - `set MESH_RUNNER_AUTH_TOKEN=<same as MESH_RUNNER_TOKEN>`
   - `set ISSUE_MESH_READONLY_MAX_WORKERS=12`
   - `set ISSUE_MESH_MAX_WORKERS_CAP=12`
   - `set CODEX_AUDIT_GATEWAY_ONLY=false`
   - `set CODEX_CANONICAL_PROVIDER=newapi-192.168.232.141-3000-stable`
   - `set CODEX_READONLY_LANE=codex-readonly`
   - `set CODEX_STABLE_LANE=codex-stable`
   - `set CODEX_READONLY_PROVIDER_ALLOWLIST=newapi-192.168.232.141-3000-ro-a,newapi-192.168.232.141-3000-ro-b,newapi-192.168.232.141-3000-ro-c,newapi-192.168.232.141-3000-ro-d`
   - `python -m uvicorn automation.mesh_runner.app:app --host 0.0.0.0 --port 8093`
   - `set PROMOTE_PREP_AUTH_TOKEN=<same as PROMOTE_PREP_TOKEN>`
   - `set PROMOTE_PREP_NEW_API_BASE_URL=http://192.168.232.141:3000`
   - `set PROMOTE_PREP_NEW_API_TOKEN=<same as NEW_API_TOKEN>`
   - `set PROMOTE_PREP_REDIS_URL=<redis://host:port/db>`
   - `python -m uvicorn automation.promote_prep.app:app --host 0.0.0.0 --port 8094`

8. Start the Loop Controller for autonomous fix-loop orchestration:

   - `set LOOP_CONTROLLER_AUTH_TOKEN=<same as LOOP_CONTROLLER_TOKEN>`
   - `set MESH_RUNNER_BASE_URL=http://127.0.0.1:8093`
   - `set PROMOTE_PREP_BASE_URL=http://127.0.0.1:8094`
   - `set WRITEBACK_A_BASE_URL=http://127.0.0.1:8092`
   - `set WRITEBACK_B_BASE_URL=http://127.0.0.1:8095`
   - `set FIX_GOAL_CONSECUTIVE=10`
   - `set AUDIT_INTERVAL_SECONDS=300`
   - `set MONITOR_INTERVAL_SECONDS=1800`
   - `python -m uvicorn automation.loop_controller.app:app --host 0.0.0.0 --port 8096`

9. Start the Kestra stack on the Ubuntu control-plane host:

   - `powershell -ExecutionPolicy Bypass -File automation/deploy/start-kestra.ps1`

10. Start the Ubuntu bridge proxy when the Windows-hosted services must be exposed to Kestra via the canonical `181xx` ports:

   - `docker run -d --name yanbao-windows-bridge --restart unless-stopped --network host -v /home/hugh/yanbao/automation/deploy/nginx.windows-bridge.conf:/etc/nginx/nginx.conf:ro nginx:1.27-alpine`

10. Check stack:

   - `powershell -ExecutionPolicy Bypass -File automation/deploy/check-stack.ps1`
   - `powershell -ExecutionPolicy Bypass -File automation/deploy/check-stack.ps1 -CheckExternalServices`

11. Import flows from:

   - `automation/kestra/flows/yanbao_health_checks.yml`
   - `automation/kestra/flows/yanbao_report_writeback_orchestration.yml`
   - `automation/kestra/flows/yanbao_issue_mesh_audit.yml`
   - `automation/kestra/flows/yanbao_issue_mesh_status_note_promote.yml`
   - `automation/kestra/flows/yanbao_issue_mesh_current_layer_promote.yml`
   - `automation/kestra/flows/yanbao_issue_mesh_code_fix_wave.yml`
   - `automation/kestra/flows/yanbao_master_loop.yml`

## Flow Runtime Inputs

Provide these inputs when running flows in Kestra:

- `app_base_url`
- `internal_token`
- `new_api_base_url`
- `new_api_token`
- `writeback_a_base_url`
- `writeback_a_token`
- `writeback_b_base_url`
- `writeback_b_token`
- `mesh_runner_base_url`
- `mesh_runner_token`
- `promote_prep_base_url`
- `promote_prep_token`

For `yanbao_report_writeback_orchestration` also provide:

- `run_id` (`report-writeback-YYYYMMDD-NNN` or another stable execution id)
- `trade_date`
- `target_path`
- `patch_text`
- `base_sha256`

For `yanbao_issue_mesh_audit` also provide:

- `run_id` (`issue-mesh-YYYYMMDD-NNN`)
- `run_label`
- `benchmark_label`
- `max_workers` (`12` for the canonical readonly run)

For `yanbao_issue_mesh_status_note_promote` also provide:

- `run_id`
- `do_rollback_after_commit`

For `yanbao_issue_mesh_current_layer_promote` also provide:

- `run_id`
- `enabled`
- `do_rollback_after_commit`

For hybrid deployment:

- `app_base_url` must point from Ubuntu to the Windows-hosted Yanbao app.
- `new_api_base_url` should point to the Ubuntu-hosted New API.
- `writeback_a_base_url` must point from Ubuntu to the Windows-hosted writeback A service.
- `writeback_b_base_url` must point from Ubuntu to the Windows-hosted writeback B service.
- `mesh_runner_base_url` must point from Ubuntu to the Windows-hosted mesh-runner service.
- `promote_prep_base_url` must point from Ubuntu to the Windows-hosted shadow writer service.

## Local Constraints

- The writeback service can run locally with Python + Uvicorn.
- `start-all-services.ps1` and `watchdog.ps1` are the canonical local bootstrap path for `app + control-plane`; they inject `NO_PROXY` so `127.0.0.1`, `localhost`, and `192.168.232.*` never loop through desktop HTTP proxies.
- `start-all-services.ps1` and `watchdog.ps1` must inject the formal app-side autonomy env (`INTERNAL_CRON_TOKEN`, `AUTONOMY_LOOP_ENABLED`, `AUTONOMY_LOOP_MODE`, `AUTONOMY_LOOP_FIX_GOAL`, `AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS`, `AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS`) in addition to the helper-service endpoints/tokens.
- The Windows-hosted app must inherit the same `MESH_RUNNER_*`, `PROMOTE_PREP_*`, `WRITEBACK_*`, and `NEW_API_*` env contract as the standalone control-plane services; otherwise the formal in-app autonomy runtime will fail closed on the first helper-service call.
- Writeback B is the same app binary with a different token, port, audit dir, and allow/deny policy.
- `mesh-runner` and `promote_prep` are intentionally outside the Ubuntu compose stack in this stage because they need direct access to the real `D:\yanbao` repository.
- The readonly issue mesh flow now uses `submit -> poll` for both mesh execution and shadow-intent handling; it no longer calls the sync shadow endpoint directly in the main path.
- Controller-only promote flows use the formal `triage -> preview -> auto-commit` path.
- `promote_prep /v1/triage` is the formal approval gate for `status-note/current-layer`; `promote_prep /v1/triage/writeback` is the formal approval gate for report/code/doc writeback and both fail-close on non-`allow` decisions.
- Formal writeback deployments should start writeback services with `WRITEBACK_REQUIRE_TRIAGE=true`, so `commit` requests must carry a matching `triage_record_id`.
- Writeback B should also set `WRITEBACK_REQUIRE_FENCING=true`, so `commit` / `batch-commit` / `rollback` fail closed unless both `lease_id` and `fencing_token` are present.
- Formal issue mesh runs must inject a controller-generated `run_id` in the format `issue-mesh-YYYYMMDD-NNN`; do not use Kestra execution ids as the formal run id.
- Canonical worker files live under `runtime/issue_mesh/<run_id>/shard_XX/{task_spec.json,result.json}` and canonical shadow outputs live under `docs/_temp/issue_mesh_shadow/<run_id>/{summary.md,bundle.json}`.
- Kestra itself is delivered here as Docker Compose assets; if the host lacks `docker`, only static validation is possible.
- The referenced New API must be reachable from the machine running Kestra, otherwise the flow will fail at the `new_api_models` health check.
- `host.docker.internal:18100/18192/18193/18194/18195` is the canonical bridge layout for the currently validated hybrid deployment.
- `automation/deploy/nginx.windows-bridge.conf` assumes the Windows host is reachable from Ubuntu as `192.168.232.1`.
- The Compose stack now pins the Kestra image tag (`kestra/kestra:v1.3.2`) instead of using `latest`, and still sets explicit Basic Auth credentials instead of relying on `enabled: false`.

## Guardrails

- Writeback A may target `app/**` and `tests/**`, but only after `promote_prep /v1/triage/writeback` returns `allow`.
- Writeback A must not point writeback targets to:
  - `docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md`
  - `output/junit.xml`
  - `app/governance/catalog_snapshot.json`
  - `output/blind_spot_audit.json`
  - `github/automation/continuous_audit/latest_run.json`
- The readonly issue-mesh flow may write `docs/_temp/issue_mesh_shadow/**`, but it must not write official shared artifacts or `docs/core/22_*`.
- Writeback B resolves its policy from `automation/control_plane/current_state.json`.
- In `infra` mode, Writeback B may only point to:
   - `automation/control_plane/current_state.json`
   - `automation/control_plane/current_status.md`
- In `infra` mode, Writeback B must not point to:
   - `docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md`
   - `output/junit.xml`
   - `app/governance/catalog_snapshot.json`
   - `output/blind_spot_audit.json`
   - `github/automation/continuous_audit/latest_run.json`
- In `doc22` mode, Writeback B may only point to:
   - `docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md`
- Writeback B must set `WRITEBACK_REQUIRE_FENCING=true`; every formal promote `commit` / `batch-commit` / `rollback` must carry the lease pair.
- The controller-only promote flows may only preview/commit against `docs/core/22_鍏ㄩ噺鍔熻兘杩涘害鎬昏〃_v7_绮惧.md`, and the commit must pass `promote_prep /v1/triage`.

