from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
import pytest

_infra_missing = pytest.mark.xfail(
    reason="LiteLLM/kestra_stack/scripts/ 未部署到工作区", strict=False)


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@_infra_missing
def test_build_issue_mesh_manifest_writes_twelve_analysis_tasks(tmp_path: Path):
    module = _load_module(
        Path("LiteLLM/kestra_stack/scripts/build_issue_mesh_manifest.py").resolve(),
        "build_issue_mesh_manifest_under_test",
    )
    output_path = tmp_path / "manifest.json"

    manifest = module.build_manifest(["newapi-192.168.232.141-3000"], 12)
    output_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["max_workers"] == 12
    assert written["provider_allowlist"] == ["newapi-192.168.232.141-3000"]
    assert len(written["tasks"]) == 12
    assert all(task["task_kind"] == "analysis" for task in written["tasks"])


@_infra_missing
def test_render_mesh_summary_outputs_task_blocks(tmp_path: Path):
    module = _load_module(
        Path("LiteLLM/kestra_stack/scripts/render_mesh_summary.py").resolve(),
        "render_mesh_summary_under_test",
    )
    last_message = tmp_path / "worker.txt"
    last_message.write_text("worker output", encoding="utf-8")
    summary = {
        "run_id": "run-1",
        "execution_mode": "mesh",
        "success": True,
        "task_count": 1,
        "started_at": "2026-03-27T00:00:00+00:00",
        "finished_at": "2026-03-27T00:10:00+00:00",
        "tasks": [
            {
                "task_id": "truth-lineage",
                "goal": "真实性/血缘止血复核",
                "success": True,
                "selected_provider": "newapi-192.168.232.141-3000",
                "workspace_kind": "shared",
                "attempts": [{"last_message_path": str(last_message)}],
            }
        ],
    }

    markdown = module.build_markdown(summary)

    assert "# Codex Mesh Summary: run-1" in markdown
    assert "### truth-lineage" in markdown
    assert "worker output" in markdown
