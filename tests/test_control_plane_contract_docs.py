from __future__ import annotations

from pathlib import Path


def test_api_contract_doc_covers_internal_control_plane_endpoints():
    text = Path("docs/core/05_API与数据契约.md").read_text(encoding="utf-8")

    assert "### §6.9 内部控制平面：运行时门禁" in text
    assert "`GET /api/v1/internal/runtime/gates`" in text
    assert "`data.runtime_live_recovery`" in text
    assert "`data.shared_artifact_promote`" in text
    assert "`data.llm_router`" in text

    assert "### §6.10 内部控制平面：审计上下文" in text
    assert "`GET /api/v1/internal/audit/context`" in text
    assert "`data.runtime_gates: object`" in text
    assert "`progress_doc_path: string`" in text
    assert "`analysis_lens_doc_path: string`" in text
    assert "`data.public_runtime_status: string|null`" in text
