from __future__ import annotations

from datetime import datetime, timezone

from app.models import Base
from app.services import report_engine


def test_collect_topics_bridges_legacy_hotspot_to_ssot_tables(db_session, monkeypatch):
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    monkeypatch.setattr(report_engine, "_industry_keywords", lambda _stock_code: [])
    monkeypatch.setattr(
        report_engine,
        "link_topic_to_stock",
        lambda _title, _stock_code, **_kwargs: {"relevance_score": 0.91, "match_method": "title_match"},
    )
    monkeypatch.setattr(
        report_engine,
        "enrich_topic",
        lambda topic: {
            "topic_id": topic["topic_id"],
            "canonical_topic": topic["title"],
            "heat_score": 87.5,
            "sentiment_score": 0.2,
            "event_type": "policy",
            "decay_weight": 0.9,
        },
    )

    report_engine.collect_topics(
        db_session,
        stock_code="600519.SH",
        raw_topics=[
            {
                "topic_id": "topic-bridge-1",
                "platform": "weibo",
                "rank": 1,
                "title": "热点桥接测试话题",
                "raw_heat": "9999",
                "fetch_time": now,
                "source_url": "https://example.com/topic/1",
            }
        ],
    )

    hotspot_raw = Base.metadata.tables["hotspot_raw"]
    hotspot_norm = Base.metadata.tables["hotspot_normalized"]
    hotspot_link = Base.metadata.tables["hotspot_stock_link"]
    ssot_item = Base.metadata.tables["market_hotspot_item"]
    ssot_link = Base.metadata.tables["market_hotspot_item_stock_link"]

    assert db_session.execute(hotspot_raw.select()).mappings().first() is not None
    assert db_session.execute(hotspot_norm.select()).mappings().first() is not None
    assert db_session.execute(hotspot_link.select()).mappings().first() is not None

    ssot_item_row = db_session.execute(ssot_item.select()).mappings().first()
    assert ssot_item_row is not None
    assert ssot_item_row["topic_title"] == "热点桥接测试话题"

    ssot_link_row = db_session.execute(ssot_link.select()).mappings().first()
    assert ssot_link_row is not None
    assert ssot_link_row["stock_code"] == "600519.SH"
    assert ssot_link_row["relation_role"] == "primary"
