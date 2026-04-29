from __future__ import annotations

from pathlib import Path

from app.models import PaymentWebhookEvent
from app.ssot_schema import INDEX_SPECS

ROOT = Path(__file__).resolve().parents[1]
SSOT_04_PATH = ROOT / "docs" / "core" / "04_数据治理与血缘.md"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig")


def _extract_table_block(doc_text: str, table_name: str) -> str:
    heading = f"#### 表：`{table_name}`"
    start = doc_text.find(heading)
    assert start >= 0, f"missing table block for {table_name} in {SSOT_04_PATH.as_posix()}"
    rest = doc_text[start + len(heading):]
    next_heading = rest.find("\n#### 表：`")
    if next_heading >= 0:
        return rest[:next_heading]
    return rest


def _payment_webhook_doc_contract() -> tuple[bool, bool]:
    doc_text = _read_text(SSOT_04_PATH)
    heading = f"#### 表：`payment_webhook_event`"
    if doc_text.find(heading) < 0:
        return False, False
    block = _extract_table_block(doc_text, "payment_webhook_event")
    has_request_id = "| `request_id` |" in block
    has_request_index = "`idx_payment_webhook_event_request(request_id, received_at)`" in block
    return has_request_id, has_request_index


def test_payment_webhook_event_runtime_and_schema_keep_request_id_backlink():
    runtime_indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in PaymentWebhookEvent.__table__.indexes
    }
    schema_indexes = dict(INDEX_SPECS["payment_webhook_event"])

    assert "request_id" in PaymentWebhookEvent.__table__.c
    assert runtime_indexes["idx_payment_webhook_event_request"] == ("request_id", "received_at")
    assert schema_indexes["idx_payment_webhook_event_request"] == ("request_id", "received_at")


def test_payment_webhook_event_ssot04_request_id_alignment_is_all_or_nothing():
    has_request_id, has_request_index = _payment_webhook_doc_contract()
    assert has_request_id == has_request_index, (
        "docs/core/04_数据治理与血缘.md must add payment_webhook_event.request_id and "
        "idx_payment_webhook_event_request(request_id, received_at) together."
    )


def test_payment_webhook_event_request_id_ssot04_is_aligned_with_runtime_schema():
    has_request_id, has_request_index = _payment_webhook_doc_contract()
    assert has_request_id, "docs/core/04_数据治理与血缘.md must document payment_webhook_event.request_id."
    assert has_request_index, (
        "docs/core/04_数据治理与血缘.md must document "
        "idx_payment_webhook_event_request(request_id, received_at)."
    )
