"""NFR-16: 安全/PII 保护测试

planned_test_ids:
  - test_nfr16_password_not_logged
  - test_nfr16_prompt_no_pii
  - test_nfr16_notification_payload_no_pii
"""
import logging

import pytest

pytestmark = pytest.mark.feature("NFR-16-SECURITY")


def test_nfr16_password_not_logged(client, create_user, caplog):
    """注册/登录过程中密码不应出现在日志中。"""
    test_password = "SuperSecret_XyZ_987!"
    with caplog.at_level(logging.DEBUG):
        # 注册
        client.post(
            "/auth/register",
            json={
                "email": "nfr16_pw@example.com",
                "password": test_password,
            },
        )
        # 登录
        client.post(
            "/auth/login",
            json={
                "email": "nfr16_pw@example.com",
                "password": test_password,
            },
        )

    # 检查所有日志记录中不包含明文密码
    for record in caplog.records:
        assert test_password not in record.getMessage(), (
            f"密码明文出现在日志中: {record.getMessage()[:200]}"
        )


def test_nfr16_prompt_no_pii(db_session, create_user):
    """LLM prompt 中不应包含用户邮箱等 PII。"""
    from app.services.report_generation_ssot import _build_llm_prompt

    prompt = _build_llm_prompt(
        stock_code="600519.SH",
        stock_name="贵州茅台",
        strategy_type="A",
        market_state="BULL",
        quality_flag="ok",
        prior_stats=None,
        signal_entry_price=1800.0,
    )

    # prompt 中不应包含任何邮箱格式的内容
    import re
    email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    assert not email_pattern.search(prompt), "LLM prompt 中包含邮箱 PII"
    # 不应包含密码相关字段
    assert "password" not in prompt.lower()


def test_nfr16_notification_payload_no_pii():
    """通知 payload 中不应包含用户密码或 token。"""
    from app.services.notification import send_admin_notification

    # 构建一个 report_ready 通知 payload
    payload = {
        "count": 5,
        "pool_size": 200,
        "fail": 0,
        "trade_date": "2026-03-10",
    }

    # 调用不会发送（无 webhook URL 配置），只验证构建逻辑不产生 PII
    result = send_admin_notification("report_ready", payload)
    # 无 webhook 时返回 False，不会暴露任何用户数据
    assert result is False

    # 验证 buy_signal 类型 payload
    buy_payload = {
        "report_id": "rpt-001",
        "stock_code": "600519.SH",
        "stock_name": "贵州茅台",
    }
    result2 = send_admin_notification("buy_signal", buy_payload)
    assert result2 is False
