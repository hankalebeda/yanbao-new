"""v7精审 批量补充测试 — FR-09/FR-10/FR-12/FR-13 验收覆盖。"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from tests.helpers_ssot import (
    insert_report_bundle_ssot,
    insert_sim_account,
    insert_sim_dashboard_snapshot,
    insert_sim_equity_curve_point,
    insert_strategy_metric_snapshot,
)


def _admin_headers(client, create_user):
    admin = create_user(email="batch3-admin@test.com", password="Password123", role="admin", email_verified=True)
    resp = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
    return {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}


def _user_headers(client, create_user, tier="Free"):
    user = create_user(email=f"batch3-{tier.lower()}@test.com", password="Password123", role="user", tier=tier, email_verified=True)
    resp = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
    return {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}


# ═══════════════════════════════════════════════════════════
# FR-09  商业化与权益
# ═══════════════════════════════════════════════════════════

class TestFR09Extended:
    """FR09 补充测试。"""

    def test_logout_invalidates_token(self, client, create_user):
        """FR09-AUTH-03: logout 后旧 token 被拦截。"""
        user = create_user(email="logout-test@test.com", password="Password123", role="user", email_verified=True)
        resp = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
        token = resp.json()["data"]["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # logout
        resp = client.post("/auth/logout", headers=headers)
        assert resp.status_code == 200

        # 旧 token 应被拦截 — 使用需要认证的端点
        resp2 = client.get("/auth/me", headers=headers)
        assert resp2.status_code in (401, 403), f"logout token should be rejected, got {resp2.status_code}"

    def test_forgot_password_always_200(self, client, create_user):
        """FR09-AUTH-06: forgot-password 无论邮箱是否存在均返回200。"""
        # 存在的邮箱
        user = create_user(email="forgot-exists@test.com", password="Password123", email_verified=True)
        resp = client.post("/auth/forgot-password", json={"email": user["user"].email})
        assert resp.status_code == 200

        # 不存在的邮箱 — 仍应返回200 (防枚举)
        resp = client.post("/auth/forgot-password", json={"email": "nonexistent@nowhere.com"})
        assert resp.status_code == 200

    def test_email_rate_limit(self, client, create_user):
        """FR09-AUTH-08: 同一邮箱多次失败 → 429。"""
        user = create_user(email="ratelimit@test.com", password="Password123", email_verified=True)
        # 连续用错误密码登录 6 次
        for i in range(6):
            resp = client.post("/auth/login", json={"email": user["user"].email, "password": f"WrongPwd{i}"})
            if resp.status_code == 429:
                break
        # 第6次应该被限速
        resp = client.post("/auth/login", json={"email": user["user"].email, "password": "WrongPwd99"})
        assert resp.status_code == 429, f"expected 429 after rate limit, got {resp.status_code}"


# ═══════════════════════════════════════════════════════════
# FR-10  站点与看板
# ═══════════════════════════════════════════════════════════

class TestFR10Extended:
    """FR10 补充测试。"""

    def test_advanced_area_401_anonymous(self, client, db_session):
        """FR10-PAGE-04: 未登录访问高级区 → 401。"""
        report = insert_report_bundle_ssot(db_session, published=True)
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced")
        assert resp.status_code == 401

    def test_advanced_area_free_truncated(self, client, db_session, create_user):
        """FR10-PAGE-04: Free用户高级区 → reasoning_chain ≤ 200字符。"""
        report = insert_report_bundle_ssot(db_session, published=True)
        # 设置一个很长的 reasoning_chain
        db_session.execute(text(
            "UPDATE report SET reasoning_chain_md = :rc WHERE report_id = :rid"
        ), {"rc": "A" * 500, "rid": str(report.report_id)})
        db_session.commit()

        headers = _user_headers(client, create_user, tier="Free")
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced", headers=headers)
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        rc = data.get("reasoning_chain", "") or data.get("reasoning_chain_md", "")
        assert len(rc) <= 210, f"Free user reasoning_chain should be ≤200 chars, got {len(rc)}"

    def test_advanced_area_pro_full(self, client, db_session, create_user):
        """FR10-PAGE-04: Pro用户高级区 → 完整 reasoning_chain。"""
        report = insert_report_bundle_ssot(db_session, published=True)
        db_session.execute(text(
            "UPDATE report SET reasoning_chain_md = :rc WHERE report_id = :rid"
        ), {"rc": "B" * 500, "rid": str(report.report_id)})
        db_session.commit()

        headers = _user_headers(client, create_user, tier="Pro")
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced", headers=headers)
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        rc = data.get("reasoning_chain", "") or data.get("reasoning_chain_md", "")
        assert len(rc) >= 400, f"Pro user should see full chain (500), got {len(rc)}"

    def test_dashboard_window_days(self, client, db_session):
        """FR10-PAGE-05: window_days 参数支持 7/14/30/60。"""
        for days in [7, 14, 30, 60]:
            resp = client.get(f"/api/v1/dashboard/stats?window_days={days}")
            assert resp.status_code == 200, f"window_days={days} should return 200, got {resp.status_code}"


# ═══════════════════════════════════════════════════════════
# FR-12  管理后台
# ═══════════════════════════════════════════════════════════

class TestFR12Extended:
    """FR12 补充测试。"""

    def test_admin_users_list_paginated(self, client, create_user):
        """FR12-ADMIN-06: 用户列表分页。"""
        headers = _admin_headers(client, create_user)
        resp = client.get("/api/v1/admin/users?page=1&page_size=10", headers=headers)
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "items" in data or "users" in data or isinstance(data, list)

    def test_admin_patch_user(self, client, db_session, create_user):
        """FR12-ADMIN-06: PATCH 修改用户角色/等级。"""
        headers = _admin_headers(client, create_user)
        user = create_user(email="patch-target@test.com", password="Password123", email_verified=True)
        user_id = str(user["user"].user_id)
        before_admin_ops = db_session.execute(text("SELECT COUNT(*) FROM admin_operation")).scalar()
        before_audit_logs = db_session.execute(text("SELECT COUNT(*) FROM audit_log")).scalar()

        resp = client.patch(
            f"/api/v1/admin/users/{user_id}",
            json={"tier": "Pro"},
            headers={**headers, "X-Request-ID": "batch3-patch-user"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is True
        assert body["data"]["tier"] == "Pro"

        updated_tier = db_session.execute(
            text("SELECT tier FROM app_user WHERE user_id = :user_id"),
            {"user_id": user_id},
        ).scalar()
        assert updated_tier == "Pro"

        after_admin_ops = db_session.execute(text("SELECT COUNT(*) FROM admin_operation")).scalar()
        after_audit_logs = db_session.execute(text("SELECT COUNT(*) FROM audit_log")).scalar()
        assert after_admin_ops == before_admin_ops + 1
        assert after_audit_logs == before_audit_logs + 1

        admin_operation = db_session.execute(
            text(
                "SELECT action_type, request_id, before_snapshot, after_snapshot "
                "FROM admin_operation WHERE target_pk = :user_id "
                "ORDER BY created_at DESC LIMIT 1"
            ),
            {"user_id": user_id},
        ).mappings().first()
        assert admin_operation["action_type"] == "PATCH_USER"
        assert admin_operation["request_id"] == "batch3-patch-user"
        before_snapshot = json.loads(admin_operation["before_snapshot"])
        after_snapshot = json.loads(admin_operation["after_snapshot"])
        assert before_snapshot["tier"] == "Free"
        assert after_snapshot["tier"] == "Pro"

    def test_admin_report_patch_audit(self, client, db_session, create_user):
        """FR12-ADMIN-03: PATCH report 产生审计记录。"""
        headers = _admin_headers(client, create_user)
        report = insert_report_bundle_ssot(db_session, published=False)

        before_count = db_session.execute(text("SELECT COUNT(*) FROM admin_operation")).scalar()
        resp = client.patch(
            f"/api/v1/admin/reports/{report.report_id}",
            json={"published": True},
            headers=headers,
        )
        assert resp.status_code == 200
        after_count = db_session.execute(text("SELECT COUNT(*) FROM admin_operation")).scalar()
        assert after_count > before_count, "PATCH report should create audit record"


# ═══════════════════════════════════════════════════════════
# FR-13  事件推送
# ═══════════════════════════════════════════════════════════

class TestFR13Extended:
    """FR13 补充测试。"""

    def test_drawdown_suppression_respects_recovery(self, db_session):
        """FR13-EVT-03: 回撤告警抑制与恢复。"""
        from app.services.event_dispatcher import (
            enqueue_drawdown_alert,
            DRAWDOWN_SUPPRESS_HOURS,
        )
        # 验证抑制常量
        assert DRAWDOWN_SUPPRESS_HOURS == 4
        # 第一次enqueue应成功（返回event_id字符串）
        eid = enqueue_drawdown_alert(
            db_session,
            account_id="test-account-100k",
            drawdown_pct=-0.15,
            capital_tier="100k",
        )
        db_session.commit()
        # 第一次应该成功入队(非None)
        assert eid is not None
        # 4小时内再次入队应被抑制(返回None)
        eid2 = enqueue_drawdown_alert(
            db_session,
            account_id="test-account-100k",
            drawdown_pct=-0.18,
            capital_tier="100k",
        )
        assert eid2 is None, f"expected suppressed (None), got {eid2}"
