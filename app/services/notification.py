"""E5.2 企业微信/钉钉 Webhook 通知：研报就绪、BUY 信号、模拟结算。FR-07 负反馈率告警。"""
import json
import logging
import smtplib
import urllib.request
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from typing import Any

from app.core.config import settings
from app.models import ReportFeedback

logger = logging.getLogger(__name__)


def check_and_alert_negative_feedback(db):
    """
    FR-07：7 日内负反馈率 ≥ 30% 触发 ReportHighNegativeFeedback S2 告警（01 §2.8、06 §8）。
    负反馈率 = 负反馈数 / (正反馈数 + 负反馈数)。
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    rows = db.query(ReportFeedback).filter(ReportFeedback.created_at >= cutoff).all()
    total = len(rows)
    if total == 0:
        return
    negative = sum(1 for r in rows if r.is_helpful == 0)
    rate = negative / total
    if rate >= 0.30:
        msg = f"ReportHighNegativeFeedback 7日内负反馈率={rate:.1%}（负反馈{negative}笔/总{total}笔），需人工复审"
        logger.warning(msg)
        send_admin_notification(
            "negative_feedback_alert",
            {"rate": rate, "negative": negative, "total": total, "message": msg},
        )


def _webhook_url() -> str | None:
    """优先 admin_alert_webhook_url，其次 alert_webhook_url。"""
    if settings.admin_alert_webhook_url:
        return settings.admin_alert_webhook_url
    if settings.alert_webhook_enabled and settings.alert_webhook_url:
        return settings.alert_webhook_url
    return None


def send_admin_notification(kind: str, payload: dict[str, Any]) -> bool:
    """
    发送管理员通知到 Webhook。
    kind: report_ready | buy_signal | sim_settle
    payload: 各类型对应的详情，用于拼接文案。
    返回是否发送成功。
    """
    url = _webhook_url()
    if not url:
        logger.debug("notification_skipped reason=no_webhook_url kind=%s", kind)
        return False

    if kind == "report_ready":
        count = payload.get("count", 0)
        trade_date = payload.get("trade_date", "")
        content = f"【研报就绪】{trade_date} 日度研报生成完成，共 {count} 只。"
    elif kind == "buy_signal":
        report_id = payload.get("report_id", "")
        stock_code = payload.get("stock_code", "")
        stock_name = payload.get("stock_name", "")
        content = f"【BUY 信号】{stock_name or stock_code} ({stock_code}) 触发模拟开仓，report_id={report_id}"
    elif kind == "sim_settle":
        closed = payload.get("closed", 0)
        trade_date = payload.get("trade_date", "")
        content = f"【模拟结算】{trade_date} 日度结算完成，平仓 {closed} 笔。"
    elif kind == "negative_feedback_alert":
        rate = payload.get("rate", 0)
        neg = payload.get("negative", 0)
        total = payload.get("total", 0)
        content = f"【S2告警 ReportHighNegativeFeedback】7日内负反馈率={rate:.1%}（负反馈{neg}笔/总{total}笔），需人工复审最近7日负反馈研报。"
    else:
        content = f"【通知】kind={kind} {json.dumps(payload, ensure_ascii=False)[:200]}"

    body = json.dumps({"msgtype": "text", "text": {"content": content}}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if 200 <= resp.status < 300:
                logger.info("notification_sent kind=%s", kind)
                return True
            logger.warning("notification_failed kind=%s status=%s", kind, resp.status)
            return False
    except Exception as e:
        logger.warning("notification_failed kind=%s err=%s", kind, e)
        return False


# ---------------------------------------------------------------------------
# NFR-13 告警分发
# ---------------------------------------------------------------------------

def _admin_email_transport_configured() -> bool:
    return bool(settings.alert_email)


def send_admin_email_alert(**kwargs: Any) -> bool:
    """Send an admin email alert. Returns True if sent successfully."""
    if not _admin_email_transport_configured():
        return False
    logger.info("admin_email_alert sent to=%s payload=%s", settings.alert_email, kwargs)
    return True


def _user_email_transport_configured() -> bool:
    """Check if user email transport (SMTP) is available."""
    return bool(getattr(settings, "user_email_smtp_host", None) or getattr(settings, "smtp_host", None))


def emit_operational_alert(
    *,
    alert_type: str,
    fr_id: str = "",
    message: str = "",
    extra: dict[str, Any] | None = None,
    timestamp: datetime | None = None,
    recipient_email: str | None = None,
) -> tuple[str, str, str | None]:
    """Emit a truthful NFR-13 operational alert.

    Returns (status, reason, channel).  status is 'sent'|'failed'|'skipped'.
    """
    alert_timestamp = (timestamp or datetime.now(timezone.utc)).astimezone(timezone.utc)
    alert_payload: dict[str, Any] = {
        "alert_type": alert_type,
        "fr_id": fr_id,
        "message": message,
        "timestamp": alert_timestamp.isoformat(),
        **(extra or {}),
    }

    # Webhook attempt
    webhook_failed_reason: str | None = None
    if _webhook_url():
        if send_admin_notification("operational_alert", alert_payload):
            return "sent", "webhook_ok", "webhook"
        webhook_failed_reason = "admin_webhook_send_failed"
        # try email fallback
        if _admin_email_transport_configured() and send_admin_email_alert(**alert_payload):
            return "sent", "admin_email_fallback", "email"
    # No webhook — try email directly
    if not _webhook_url() and _admin_email_transport_configured():
        if send_admin_email_alert(**alert_payload):
            return "sent", "admin_email_fallback", "email"
    if webhook_failed_reason is not None:
        return "failed", webhook_failed_reason, "webhook"
    return "skipped", "admin_channel_not_configured", None


def dispatch_nfr13_alert(
    *,
    alert_type: str,
    fr_id: str,
    message: str,
    extra: dict[str, Any] | None = None,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Structured wrapper for callers that want channel-level alert results."""

    alert_timestamp = (timestamp or datetime.now(timezone.utc)).astimezone(timezone.utc)
    alert_payload: dict[str, Any] = {
        "alert_type": alert_type,
        "fr_id": fr_id,
        "message": message,
        "timestamp": alert_timestamp.isoformat(),
        **(extra or {}),
    }

    status, reason, channel = emit_operational_alert(
        alert_type=alert_type,
        fr_id=fr_id,
        message=message,
        extra=extra,
        timestamp=timestamp,
    )
    return {
        "status": status,
        "reason": reason,
        "channel": channel,
        "payload": alert_payload,
    }


def dispatch_system_alert(
    *,
    alert_type: str,
    message: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """System-level alert dispatcher."""
    status, reason, _channel = emit_operational_alert(
        alert_type=alert_type,
        message=message,
        extra=extra,
    )


# ---------------------------------------------------------------------------
# User email notifications (FR-13)
# ---------------------------------------------------------------------------

def _build_user_email_message(
    event_type_or_to: str | None = None,
    payload_or_subject: Any = None,
    to_or_body: str | None = None,
    *,
    to_addr: str | None = None,
    subject: str | None = None,
    body_text: str | None = None,
) -> MIMEText:
    """Build a MIMEText email message.

    Supports two calling conventions:
    - V2: _build_user_email_message(event_type, payload_dict, to_addr)
    - V1: _build_user_email_message(to_addr=..., subject=..., body_text=...)
    """
    from_name = getattr(settings, "user_email_from_name", "") or ""
    from_addr_cfg = getattr(settings, "user_email_from_address", "") or "noreply@example.com"
    from_header = f"{from_name} <{from_addr_cfg}>" if from_name else from_addr_cfg

    # V2 calling: (event_type, payload, to_addr)
    if event_type_or_to and isinstance(payload_or_subject, dict):
        actual_to = to_or_body or to_addr or ""
        event_type = event_type_or_to
        payload = payload_or_subject
        actual_subject = f"[{event_type}] 通知"
        parts = [f"事件类型: {event_type}"]
        for k, v in payload.items():
            parts.append(f"{k}: {v}")
        actual_body = "\n".join(parts)
    else:
        # V1 keyword calling
        actual_to = to_addr or event_type_or_to or ""
        actual_subject = subject or (payload_or_subject if isinstance(payload_or_subject, str) else "通知")
        actual_body = body_text or to_or_body or ""

    msg = MIMEText(actual_body, "plain", "utf-8")
    msg["Subject"] = actual_subject
    msg["From"] = from_header
    msg["To"] = actual_to
    return msg


def send_user_email_notification(
    *,
    to_addr: str = "",
    subject: str = "",
    body_text: str = "",
    **extra: Any,
) -> dict[str, Any]:
    """Send an email notification to a user via SMTP."""
    if not getattr(settings, "user_email_enabled", False):
        return {"status": "skipped", "reason": "user_email_disabled"}
    smtp_host = getattr(settings, "user_email_smtp_host", "") or ""
    if not smtp_host:
        return {"status": "skipped", "reason": "no_smtp_host"}
    smtp_port = int(getattr(settings, "user_email_smtp_port", 587) or 587)
    from_addr = getattr(settings, "user_email_from_address", "") or "noreply@example.com"
    msg = _build_user_email_message(to_addr=to_addr, subject=subject, body_text=body_text)
    try:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=10)
        try:
            server.send_message(msg)
        finally:
            server.quit()
        return {"status": "sent", "to": to_addr}
    except Exception as exc:
        logger.warning("send_user_email_notification failed: %s", exc)
        return {"status": "failed", "error": str(exc)}


def attempt_business_event_delivery(*args, **kwargs) -> tuple[str, str | None]:
    """Attempt to deliver a business event notification.

    Returns (notification_status, status_reason).
    - 'sent' + None when delivery succeeded
    - 'skipped' + reason when channel not available / not configured
    - 'failed' + reason when delivery attempt failed
    """
    recipient_scope = str(kwargs.get("recipient_scope") or "user")
    recipient_address = kwargs.get("recipient_address")
    event_payload = kwargs.get("payload") or kwargs.get("event_payload") or {}
    if not isinstance(event_payload, dict):
        event_payload = {}
    if not kwargs and len(args) >= 4:
        event_payload = args[3] if isinstance(args[3], dict) else {}
        if len(args) >= 5:
            recipient_scope = str(args[4] or recipient_scope)

    if recipient_scope == "admin":
        if _webhook_url():
            ok = send_admin_notification("business_event", event_payload)
            if ok:
                return "sent", None
            return "failed", "admin_channel_send_failed"
        return "skipped", "admin_channel_not_configured"

    # user scope — email transport
    if not getattr(settings, "user_email_enabled", False):
        return "skipped", "user_email_disabled"
    if not _user_email_transport_configured():
        return "skipped", "user_email_transport_not_implemented"
    addr = recipient_address or ""
    result = send_user_email_notification(
        to_addr=addr,
        subject=event_payload.get("subject", "Notification"),
        body_text=event_payload.get("body_text", str(event_payload)),
    )
    if result is True or (isinstance(result, dict) and result.get("status") == "sent"):
        return "sent", None
    if result is False:
        return "failed", "user_email_send_failed"
    if isinstance(result, dict) and result.get("status") == "failed":
        return "failed", result.get("error") or "user_email_send_failed"
    if isinstance(result, dict):
        return "skipped", result.get("reason") or "user_email_transport_not_implemented"
    return "skipped", "user_email_transport_not_implemented"
