"""FR04-DATA-02 热点富化 (POST /api/v1/internal/hotspot/enrich) 验收测试
SSOT: 01 §FR04-DATA-02, 05 §/internal/hotspot/enrich
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.models import Base
from app.services.hotspot import enrich_topic


def _seed_hotspot_items(db, *, count=2):
    """向 market_hotspot_item 插入 count 条近 24h 条目。"""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    hotspot_t = Base.metadata.tables["market_hotspot_item"]
    titles = [
        "AI芯片产业链增长 600519.SH",
        "白酒消费回暖 贵州茅台",
        "银行板块承压 平安银行",
    ]
    for i in range(count):
        db.execute(hotspot_t.insert().values(
            hotspot_item_id=str(uuid4()),
            batch_id=str(uuid4()),
            source_name="weibo",
            merged_rank=i + 1,
            topic_title=titles[i % len(titles)],
            source_url=f"https://example.com/topic/{i}",
            fetch_time=now,
            quality_flag="ok",
            created_at=now,
        ))
    db.commit()


@pytest.mark.feature("FR04-DATA-02")
class TestHotspotEnrich:
    def test_enrich_topic_populates_real_fields(self):
        topic = {
            "topic_id": "topic-1",
            "title": "AI芯片产业链增长 贵州茅台",
            "heat_score": 88,
            "fetch_time": datetime.now(timezone.utc),
            "industry": "半导体",
            "last_price": 1250.0,
            "prev_close": 1200.0,
            "circulating_shares": 1_000_000,
            "stock_code": "600519.SH",
            "stock_name": "贵州茅台",
        }

        enriched = enrich_topic(topic)

        assert enriched["canonical_topic"] == topic["title"]
        assert enriched["industry"] == "半导体"
        assert enriched["market_cap"] == 1_250_000_000.0
        assert enriched["change_pct"] == pytest.approx(4.1667, abs=1e-4)
        assert enriched["stock_code"] == "600519.SH"
        assert enriched["stock_name"] == "贵州茅台"
        assert enriched["last_price"] == 1250.0
        assert enriched["event_type"] == "industry_chain"
        assert enriched["sentiment_score"] > 0
        assert enriched["decay_weight"] == 1.0

    def test_enrich_returns_enriched_count(self, client, db_session, internal_headers):
        """有候选条目时返回 enriched > 0 及字段校验。"""
        _seed_hotspot_items(db_session, count=3)
        headers = internal_headers()

        resp = client.post("/api/v1/internal/hotspot/enrich", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["total_candidates"] == 3
        assert data["enriched"] == 3
        assert isinstance(data["items"], list)
        assert len(data["items"]) == 3
        item = data["items"][0]
        assert "hotspot_item_id" in item
        assert "sentiment_score" in item
        assert "event_type" in item
        assert "decay_weight" in item
        assert item["event_type"] == "industry_chain"
        assert item["sentiment_score"] > 0
        assert item["decay_weight"] == 1.0

    def test_enrich_empty_db_returns_zero(self, client, internal_headers):
        """无候选条目时返回 enriched=0。"""
        headers = internal_headers()

        resp = client.post("/api/v1/internal/hotspot/enrich", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["enriched"] == 0
        assert data["total_candidates"] == 0

    def test_enrich_requires_auth(self, client):
        """缺少 X-Internal-Token 时返回 401。"""
        resp = client.post("/api/v1/internal/hotspot/enrich")
        assert resp.status_code == 401
