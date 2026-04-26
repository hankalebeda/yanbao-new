from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.xfail(reason="docs/core/05 尚未包含 control plane 章节 — 待行动08补写", strict=False)
def test_api_contract_doc_covers_internal_control_plane_endpoints():
    text = Path("docs/core/05_API与数据契约.md").read_text(encoding="utf-8")

    assert "### 1.3C `GET /api/v1/internal/runtime/gates`" in text
    assert "`data.runtime_live_recovery`" in text
    assert "`data.shared_artifact_promote`" in text
    assert "`data.llm_router`" in text

    assert "### 1.3D `GET /api/v1/internal/audit/context`" in text
    assert "`data.runtime_gates: object`" in text
    assert "`progress_doc_path: string`" in text
    assert "`analysis_lens_doc_path: string`" in text
    assert "`data.public_runtime_status: string|null`" in text
