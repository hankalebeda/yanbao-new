from __future__ import annotations

import json
from pathlib import Path
import pytest


@pytest.mark.xfail(reason="LiteLLM/issue_mesh/ 目录未部署到工作区", strict=False)
def test_issue_mesh_promote_hooks_active_and_controller_only():
    root = Path("LiteLLM/issue_mesh")
    protocol_text = (root / "protocol" / "formal_promote.md").read_text(encoding="utf-8")
    schema = json.loads((root / "schemas" / "promote_intent.schema.json").read_text(encoding="utf-8"))

    assert "Doc22 Mode Active" in protocol_text
    assert "controller_only_promote_intent" in protocol_text
    assert schema["properties"]["controller_only"]["const"] is True
    assert schema["properties"]["enabled"]["const"] is True
