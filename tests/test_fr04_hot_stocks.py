"""FR04-DATA-06 热股查询 (GET /api/v1/market/hot-stocks) 验收测试
SSOT: 01 §FR04-DATA-06, 05 §/market/hot-stocks
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.models import Base


def _seed_hotspot(db, *, count=1, stock_codes=None):
    """向 market_hotspot_item + stock_link 插入 count 条近 24h 热搜。"""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    hotspot_t = Base.metadata.tables["market_hotspot_item"]
    link_t = Base.metadata.tables["market_hotspot_item_stock_link"]
    codes = stock_codes or [f"60051{i}.SH" for i in range(count)]
    for i in range(count):
        hid = str(uuid4())
        db.execute(hotspot_t.insert().values(
            hotspot_item_id=hid,
            batch_id=str(uuid4()),
            source_name="weibo",
            merged_rank=i + 1,
            topic_title=f"热搜话题{i}",
            source_url=f"https://example.com/hotspot/{i}",
            fetch_time=now,
            quality_flag="ok",
            created_at=now,
        ))
        db.execute(link_t.insert().values(
            hotspot_item_stock_link_id=str(uuid4()),
            hotspot_item_id=hid,
            stock_code=codes[i % len(codes)],
            relation_role="primary",
            created_at=now,
        ))
    db.commit()


@pytest.mark.feature("FR04-DATA-06")
class TestHotStocks:
    def test_empty_returns_empty_list(self, client):
        """无热搜数据且无核心池时返回空列表。"""
        resp = client.get("/api/v1/market/hot-stocks")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        items = body["data"]["items"]
        assert isinstance(items, list)

    def test_seeded_hotspot_items_returned(self, client, db_session):
        """有热搜数据时返回正确字段。"""
        _seed_hotspot(db_session, count=1, stock_codes=["600519.SH"])

        resp = client.get("/api/v1/market/hot-stocks")
        assert resp.status_code == 200
        items = resp.json()["data"]["items"]
        assert len(items) >= 1
        item = items[0]
        assert item["stock_code"] == "600519.SH"
        assert "stock_name" in item
        assert "heat_score" in item
        assert "rank" in item
        assert "source_name" in item

    def test_limit_constrains_result_count(self, client, db_session):
        """limit=2 时最多返回 2 条。"""
        _seed_hotspot(db_session, count=5, stock_codes=[f"60000{i}.SH" for i in range(5)])

        resp = client.get("/api/v1/market/hot-stocks?limit=2")
        assert resp.status_code == 200
        items = resp.json()["data"]["items"]
        assert len(items) <= 2

    def test_limit_exceeds_max_rejected(self, client):
        """limit > 50 被 FastAPI 校验拒绝 (422)。"""
        resp = client.get("/api/v1/market/hot-stocks?limit=100")
        assert resp.status_code == 422
