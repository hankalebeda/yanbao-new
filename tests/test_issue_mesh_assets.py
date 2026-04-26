from __future__ import annotations

import json
from pathlib import Path
import pytest


@pytest.mark.xfail(reason="LiteLLM/issue_mesh/ 目录未部署到工作区", strict=False)
def test_issue_mesh_assets_exist_and_capture_required_enums():
    root = Path("LiteLLM/issue_mesh")
    assert (root / "README.md").exists()
    assert (root / "protocol" / "control_state.md").exists()
    assert (root / "protocol" / "family_catalog.json").exists()
    assert (root / "protocol" / "future_promote.md").exists()
    assert (root / "protocol" / "workspace_modes.md").exists()
    for prompt_name in (
        "truth_lineage.txt",
        "runtime_anchor.txt",
        "fr07_rebuild.txt",
        "fr06_failure_semantics.txt",
        "payment_auth_governance.txt",
        "internal_contracts.txt",
        "shared_artifacts.txt",
        "issue_registry.txt",
        "repo_governance.txt",
        "external_integration.txt",
        "display_bridge.txt",
        "execution_order.txt",
    ):
        assert (root / "prompts" / prompt_name).exists()
    assert (root / "schemas" / "promote_round.schema.json").exists()
    assert (root / "schemas" / "promote_intent.schema.json").exists()
    assert (root / "examples" / "disabled_promote_round.json").exists()

    family_catalog = json.loads((root / "protocol" / "family_catalog.json").read_text(encoding="utf-8"))
    task_schema = json.loads((root / "schemas" / "task.schema.json").read_text(encoding="utf-8"))
    claim_schema = json.loads((root / "schemas" / "claim.schema.json").read_text(encoding="utf-8"))
    journal_schema = json.loads((root / "schemas" / "journal_entry.schema.json").read_text(encoding="utf-8"))
    promote_round_schema = json.loads((root / "schemas" / "promote_round.schema.json").read_text(encoding="utf-8"))
    promote_intent_schema = json.loads((root / "schemas" / "promote_intent.schema.json").read_text(encoding="utf-8"))

    assert [item["family_id"] for item in family_catalog] == [
        "truth-lineage",
        "runtime-anchor",
        "fr07-rebuild",
        "fr06-failure-semantics",
        "payment-auth-governance",
        "internal-contracts",
        "shared-artifacts",
        "issue-registry",
        "repo-governance",
        "external-integration",
        "display-bridge",
        "execution-order",
    ]
    assert "Recovery-Rearm" in task_schema["properties"]["control_state_required"]["enum"]
    assert "Backlog-Open" in task_schema["properties"]["control_state_required"]["enum"]
    assert "readonly" in task_schema["properties"]["workspace_mode"]["enum"]
    assert "controller-only" in task_schema["properties"]["claim_mode"]["enum"]
    assert "active" in claim_schema["properties"]["status"]["enum"]
    assert "done" in journal_schema["properties"]["status"]["enum"]
    assert "disabled" in promote_round_schema["properties"]["status"]["enum"]
    assert "disabled" in promote_intent_schema["properties"]["status"]["enum"]
