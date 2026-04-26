import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.responses import JSONResponse as fastapi_json_response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.api.routes_admin import router as admin_router
from app.api.routes_auth import router as auth_router
from app.api.routes_billing import router as billing_router
from app.api.routes_dashboard import router as dashboard_router
from app.core.security import AUTH_COOKIE, get_current_user_optional
from app.api.routes_business import router as business_router
from app.api.routes_governance import features_router, governance_router
from app.api.routes_internal import router as internal_router
from app.api.routes_sim import router as sim_router
from app.core.config import settings
from app.core.display_text import humanize_public_error_detail
from app.core.db import Base, SessionLocal, engine, ensure_report_source_column, ensure_report_trade_date_column, ensure_user_phone_column, ensure_report_llm_audit_columns, ensure_app_user_admin_seed, ensure_sqlite_schema_alignment
from sqlalchemy import func, text
from app.core.error_codes import normalize_error_code
from app.core.request_context import ensure_request_id
from app.core.response import envelope
from app.models import Report
from app.services.report_view_service import (
    build_report_template_context_for_user,
    latest_report_id_for_code,
    load_report_view_payload,
    report_status_payload,
)
from app.services.reports_query import recent_report_failure
from app.services.scheduler import start_scheduler, stop_scheduler
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai-api"))
from gemini_web import router as gemini_router
from gemini_web import shutdown as shutdown_gemini_client
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai-api"))
from chatgpt_web import router as chatgpt_router
from chatgpt_web import shutdown as shutdown_chatgpt_client
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai-api"))
from deepseek_web import router as deepseek_router
from deepseek_web import shutdown as shutdown_deepseek_client
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai-api"))
from qwen_web import router as qwen_router
from qwen_web import shutdown as shutdown_qwen_client
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai-api"))
from webai import router as webai_router

try:
    from app.services.autonomy_loop_runtime import get_autonomy_loop_runtime
except ImportError:  # pragma: no cover
    get_autonomy_loop_runtime = None  # type: ignore[assignment]

_STOCK_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ)$")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # CR-20260416-03: runtime security baseline check on startup.
    # STRICT_SECURITY=true raises; otherwise log warnings but continue.
    _strict = os.environ.get("STRICT_SECURITY", "").strip().lower() in {"1", "true", "yes", "on"}
    try:
        settings.validate_runtime_security()
    except RuntimeError as _sec_err:
        if _strict:
            raise
        logger.warning("runtime security baseline warning: %s", _sec_err)
    if settings.enable_scheduler:
        start_scheduler()
        try:
            from app.services.dag_scheduler import start_timeout_watcher
            start_timeout_watcher()
        except Exception:
            pass
    yield
    try:
        from app.services.dag_scheduler import stop_timeout_watcher
        stop_timeout_watcher()
    except Exception:
        pass
    stop_scheduler()
    if settings.autonomy_loop_enabled and get_autonomy_loop_runtime is not None:
        try:
            get_autonomy_loop_runtime().shutdown(reason="app_shutdown")
        except Exception:
            pass
    await shutdown_gemini_client()
    await shutdown_chatgpt_client()
    await shutdown_deepseek_client()
    await shutdown_qwen_client()


app = FastAPI(title=settings.app_name, lifespan=lifespan)

Base.metadata.create_all(bind=engine)
ensure_sqlite_schema_alignment(engine)
ensure_report_trade_date_column()
ensure_report_source_column()
ensure_user_phone_column()
ensure_report_llm_audit_columns()  # v26 P0
ensure_app_user_admin_seed()

app.include_router(internal_router)
app.include_router(auth_router)
app.include_router(business_router)
app.include_router(sim_router)
app.include_router(admin_router)
app.include_router(billing_router)
app.include_router(dashboard_router)
app.include_router(features_router)
app.include_router(governance_router)
app.include_router(gemini_router)
app.include_router(chatgpt_router)
app.include_router(deepseek_router)
app.include_router(qwen_router)
app.include_router(webai_router)

base_dir = Path(__file__).resolve().parent
project_root = base_dir.parent
templates = Jinja2Templates(directory=str(base_dir / "web" / "templates"))
templates.env.globals["api_base"] = settings.api_prefix
app.mount("/static", StaticFiles(directory=str(base_dir / "web")), name="static")
app.mount("/demo-pages", StaticFiles(directory=str(project_root / "demo"), html=True), name="demo_pages")
# /demo 不挂载 StaticFiles，否则会拦截 /demo/report/* 动态路由；静态 demo 页请用 /demo-pages

trusted_hosts = [x.strip() for x in settings.trusted_hosts.split(",") if x.strip()]
if trusted_hosts:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted_hosts)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_stock_code(stock_code: str) -> str:
    code = (stock_code or "").strip().upper()
    # Handle bare 6-digit codes
    if re.match(r"^\d{6}$", code):
        if code.startswith(("6", "9")):
            code += ".SH"
        else:
            code += ".SZ"
    return code


def _validate_stock_code(stock_code: str) -> str:
    code = _normalize_stock_code(stock_code)
    if not _STOCK_CODE_RE.match(code):
        raise HTTPException(status_code=400, detail="invalid_stock_code")
    return code


def _latest_report_row(db, stock_code: str) -> Report | None:
    return (
        db.query(Report)
        .filter(Report.stock_code == stock_code)
        .order_by(Report.created_at.desc())
        .first()
    )


def _latest_daily_report(db, stock_code: str) -> Report | None:
    return (
        db.query(Report)
        .filter(
            Report.stock_code == stock_code,
            Report.published == True,
            Report.is_deleted == False,  # noqa: E712
            func.lower(func.coalesce(Report.quality_flag, "ok")) == "ok",
        )
        .order_by(Report.created_at.desc())
        .first()
    )


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    req_id = ensure_request_id(request.headers.get("X-Request-ID"))
    response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "index.html", {"current_user": user})


@app.get("/reports", response_class=HTMLResponse)
async def reports_list_page(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "reports_list.html", {"current_user": user})


@app.get("/reports/list", response_class=HTMLResponse)
async def reports_list_redirect():
    """兼容旧路径 → /reports"""
    return RedirectResponse(url="/reports", status_code=302)


@app.get("/report/{stock_code}/status")
async def _report_status_priority(stock_code: str):
    """Registered before catch-all to ensure correct route priority."""
    return await canonical_report_status(stock_code)


@app.get("/report/{legacy_path:path}", response_class=HTMLResponse)
async def legacy_report_redirect(legacy_path: str):
    """兼容旧路径 /report/实时研报/{code} → /reports; 单段code → redirect or 404"""
    # Multi-segment paths like /report/实时研报/600519.SH → redirect
    if "/" in legacy_path:
        return RedirectResponse(url="/reports", status_code=302)
    # Single-segment code: try to find a report and redirect
    normalized = _normalize_stock_code(legacy_path)
    db = SessionLocal()
    try:
        from app.models import Report
        report = db.query(Report).filter(Report.stock_code == normalized).order_by(Report.created_at.desc()).first()
        if report:
            return RedirectResponse(url=f"/reports/{report.report_id}", status_code=302)
    finally:
        db.close()
    # No report found → 404 with friendly HTML
    return HTMLResponse(
        content=(
            '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/>'
            '<meta name="viewport" content="width=device-width,initial-scale=1"/>'
            "<title>研报生成失败</title>"
            "<style>"
            'body{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#fff5f5;color:#4a1010;}'
            ".wrap{max-width:900px;margin:24px auto;padding:0 16px;}"
            ".card{background:#fff;border:1px solid #f2caca;border-radius:12px;padding:16px;}"
            "</style></head><body>"
            '<main><div class="wrap"><div class="card">'
            "<h2>最新研报准备中</h2>"
            f"<p>股票：<b>{normalized}</b></p>"
            "<p>这只股票的最新公开研报仍在整理中，请稍后刷新页面查看。</p>"
            "<p>可先返回研报列表查看已发布内容。</p>"
            '<p><a href="/reports">查看研报列表</a></p>'
            "</div></div></main></body></html>"
        ),
        status_code=404,
    )


@app.get("/reports/{report_id}", response_class=HTMLResponse)
async def report_detail_by_id(request: Request, report_id: str):
    """按 report_id 打开研报详情页，满足整合 §4.1 列表「详情入口」指向本行报告。"""
    db = SessionLocal()
    try:
        from app.services.reports_query import get_report_access_state_ssot, get_report_view_payload_ssot

        current_user = await get_current_user_optional(request)
        access_state = get_report_access_state_ssot(
            db,
            report_id,
            viewer_tier=getattr(current_user, "tier", None) if current_user else None,
            viewer_role=getattr(current_user, "role", None) if current_user else None,
        )
        if access_state == "hidden_by_viewer_cutoff":
            raise HTTPException(status_code=403, detail="REPORT_NOT_AVAILABLE")
        view_report = get_report_view_payload_ssot(
            db,
            report_id,
            viewer_tier=getattr(current_user, "tier", None) if current_user else None,
            viewer_role=getattr(current_user, "role", None) if current_user else None,
        )
        if not view_report:
            row = db.get(Report, report_id)
            if not row:
                raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
            if bool(getattr(row, "is_deleted", False)):
                raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
            if str(getattr(row, "quality_flag", "ok") or "ok").strip().lower() != "ok":
                raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
            view_report = dict(row.content_json or {})
            view_report.setdefault("recommendation", row.recommendation)
            view_report.setdefault("confidence", row.confidence)
            view_report.setdefault("stock_name", row.stock_name_snapshot)
            view_report.setdefault("strategy_type", row.strategy_type)
            view_report.setdefault("risk_audit_status", row.risk_audit_status)
            view_report.setdefault("risk_audit_skip_reason", row.risk_audit_skip_reason)
            view_report = await load_report_view_payload(view_report, row.stock_code)
            view_report["report_id"] = row.report_id
            view_report["created_at"] = row.created_at.isoformat() if row.created_at else view_report.get("created_at")
            view_report["trade_date"] = row.trade_date or view_report.get("trade_date")
        ctx = await build_report_template_context_for_user(request, view_report)
        return templates.TemplateResponse(request, "report_view.html", ctx)
    finally:
        db.close()


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = await get_current_user_optional(request)
    if user:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request, "login.html", {"current_user": user})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    user = await get_current_user_optional(request)
    if user:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request, "register.html", {"current_user": user})


@app.get("/subscribe", response_class=HTMLResponse)
async def subscribe_page(request: Request):
    import json as _json
    from app.services.membership import (
        get_payment_capability,
        get_plans_config,
        payment_browser_checkout_ready,
        subscription_status,
    )

    user = await get_current_user_optional(request)
    subscription_state = None
    billing_cap = None

    if user:
        db = SessionLocal()
        try:
            subscription_state = subscription_status(db, str(getattr(user, "user_id", user.id)))
        except Exception:
            subscription_state = {"tier": getattr(user, "tier", "Free"), "status": "unknown", "status_reason": "lookup_failed"}
        finally:
            db.close()

    try:
        raw_cap = get_payment_capability()
        is_mock = raw_cap.get("mock_billing", False)
        providers = []
        for p in raw_cap.get("providers", []):
            mode = "headless_mock" if is_mock else "browser_checkout"
            providers.append({"name": p, "mode": mode})
        billing_cap = {
            "provider_status": "configured" if raw_cap.get("enabled") else "unavailable",
            "browser_checkout_ready": payment_browser_checkout_ready(),
            "providers": providers,
        }
    except Exception:
        billing_cap = {"provider_status": "unavailable", "browser_checkout_ready": False, "providers": []}

    ctx = {
        "current_user": user,
        "subscription_state_json": _json.dumps(subscription_state, ensure_ascii=False, default=str) if subscription_state else "null",
        "subscription_plans_json": _json.dumps(get_plans_config(), ensure_ascii=False, default=str),
        "billing_capability_json": _json.dumps(billing_cap, ensure_ascii=False, default=str) if billing_cap else "null",
    }
    return templates.TemplateResponse(request, "subscribe.html", ctx)


@app.get("/forgot-password", response_class=HTMLResponse)
async def forgot_password_page(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "forgot_password.html", {"current_user": user})


@app.get("/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request):
    user = await get_current_user_optional(request)
    token = request.query_params.get("token", "")
    token_valid = bool(token and len(token) >= 10)
    if token_valid:
        import hashlib
        from app.models import AuthTempToken
        h = hashlib.sha256(token.encode()).hexdigest()
        db = SessionLocal()
        try:
            row = db.query(AuthTempToken).filter(
                AuthTempToken.token_hash == h,
                AuthTempToken.token_type == "PASSWORD_RESET",
            ).first()
            exp = getattr(row, "expires_at", None) if row else None
            if exp is not None and getattr(exp, "tzinfo", None) is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if not row or (exp and exp < datetime.now(timezone.utc)):
                token_valid = False
        except Exception:
            token_valid = False
        finally:
            db.close()
    return templates.TemplateResponse(request, "reset_password.html", {
        "current_user": user,
        "token_valid": token_valid,
        "reset_token": token,
    })


@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")
    return templates.TemplateResponse(request, "profile.html", {"current_user": user})


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/login?next=/admin", status_code=302)
    if (user.role or "").lower() not in ("admin", "super_admin"):
        raise HTTPException(status_code=403, detail="需管理员权限")
    return templates.TemplateResponse(request, "admin.html", {"current_user": user})


@app.get("/logout")
async def logout_page():
    """登出并重定向到首页"""
    r = RedirectResponse(url="/", status_code=302)
    r.delete_cookie(key=AUTH_COOKIE, path="/")
    return r


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "dashboard.html", {"current_user": user})


@app.get("/portfolio/sim-dashboard", response_class=HTMLResponse)
async def sim_dashboard_page(request: Request):
    """模拟实盘看板（07 §11、17 §2.4：未登录/免费不可见；付费或管理员可见）"""
    user = await get_current_user_optional(request)
    if user is None:
        return RedirectResponse(url="/login?next=/portfolio/sim-dashboard", status_code=302)
    is_logged_in = True
    is_admin = (user.role or "").strip().lower() in ("admin", "super_admin")
    tier = (getattr(user, "tier", None) or "").strip() if user else ""
    membership_level = (getattr(user, "membership_level", None) or "").strip().lower() if user else ""
    is_paid = tier.lower() not in ("free", "") or membership_level in ("monthly", "annual")
    # Paid tier with null expiry = unconfirmed → deny
    if is_paid and not is_admin and user is not None:
        tier_expires_at = getattr(user, "tier_expires_at", None)
        if tier_expires_at is None:
            raise HTTPException(status_code=403, detail="MEMBERSHIP_UNCONFIRMED")
    can_see_sim = user is not None and (is_paid or is_admin)
    ctx = {"current_user": user, "is_logged_in": is_logged_in, "can_see_sim": can_see_sim}
    if not can_see_sim:
        return templates.TemplateResponse(request, "sim_dashboard.html", ctx, status_code=403)
    return templates.TemplateResponse(request, "sim_dashboard.html", ctx)


@app.get("/demo", response_class=HTMLResponse)
async def demo_landing(request: Request):
    """演示入口：重定向到研报列表"""
    return RedirectResponse(url="/reports", status_code=302)


@app.get("/sim-dashboard", response_class=HTMLResponse)
async def sim_dashboard_redirect():
    """兼容旧路径 → 新路径"""
    return RedirectResponse(url="/portfolio/sim-dashboard", status_code=302)


@app.get("/sim", response_class=HTMLResponse)
async def sim_legacy_redirect():
    """兼容历史 sim 入口。"""
    return RedirectResponse(url="/portfolio/sim-dashboard", status_code=302)


@app.get("/sim/dashboard", response_class=HTMLResponse)
async def sim_dashboard_legacy_redirect():
    """兼容历史 sim dashboard 入口。"""
    return RedirectResponse(url="/portfolio/sim-dashboard", status_code=302)


@app.get("/features", response_class=HTMLResponse)
async def features_page(request: Request):
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/login?next=/features", status_code=302)
    if (user.role or "").lower() not in {"admin", "super_admin"}:
        raise HTTPException(status_code=403, detail="需管理员权限")

    from app.api.routes_governance import _get_catalog
    from collections import OrderedDict

    catalog = _get_catalog("live")
    catalog_bridge = {
        "progress_doc_path": "docs/core/22_全量功能进度总表_v12.md",
        "fr10_name_bridge": [],
    }
    groups: dict[str, list] = OrderedDict()
    fr_names: dict[str, str] = {}
    for feat in catalog.get("features", []):
        fid = feat.get("fr_id", "UNKNOWN")
        groups.setdefault(fid, []).append(feat)
        if fid not in fr_names:
            fr_names[fid] = feat.get("fr_name", fid)
    return templates.TemplateResponse(request, "features.html", {
        "current_user": user,
        "catalog": catalog,
        "catalog_bridge": catalog_bridge,
        "catalog_api_path": "/api/v1/features/catalog?source=live",
        "catalog_snapshot_api_path": "/api/v1/governance/catalog?source=snapshot",
        "admin_system_status_api_path": "/api/v1/admin/system-status",
        "health_api_path": "/health",
        "groups": groups,
        "fr_names": fr_names,
    })


@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "privacy.html", {"current_user": user})


@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "terms.html", {"current_user": user})


@app.get("/watchlist", response_class=HTMLResponse)
async def watchlist_page(request: Request):
    user = await get_current_user_optional(request)
    return templates.TemplateResponse(request, "watchlist.html", {"current_user": user})


@app.get("/demo/report", response_class=HTMLResponse)
async def demo_report_redirect(stock_code: str = ""):
    code = _normalize_stock_code(stock_code)
    if not code or not _STOCK_CODE_RE.match(code):
        raise HTTPException(status_code=400, detail="invalid_stock_code")
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"/report/{code}", status_code=302)


@app.get("/report", response_class=HTMLResponse)
async def report_search_redirect(stock_code: str = ""):
    """Form search redirect: /report?stock_code=... → /report/{code}"""
    code = _normalize_stock_code(stock_code)
    if not code or not _STOCK_CODE_RE.match(code):
        raise HTTPException(status_code=400, detail="invalid_stock_code")
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"/report/{code}", status_code=302)


@app.get("/demo/report/{stock_code}", response_class=HTMLResponse)
async def demo_report_compat(request: Request, stock_code: str):
    """Compat redirect: /demo/report/{code} → /report/{code}"""
    code = _normalize_stock_code(stock_code)
    if not code:
        code = stock_code
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"/report/{code}", status_code=302)


async def canonical_report_status(stock_code: str):
    """Report status by stock code — JSON envelope. (Route registered earlier as _report_status_priority)"""
    code = _normalize_stock_code(stock_code)
    if not code or not _STOCK_CODE_RE.match(code):
        return fastapi_json_response(status_code=404, content=envelope(code=4004, data=None, error_code="NOT_FOUND", message="无效的股票代码"))
    db = SessionLocal()
    try:
        latest_report_id = latest_report_id_for_code(db, code)
        status_payload = report_status_payload(code, db=db)
    finally:
        db.close()
    job = status_payload.get("job") or {}
    if job.get("ready"):
        return envelope(data={"stock_code": code, "report_id": job.get("report_id") or latest_report_id, "status": "done", "ready": True})
    return envelope(data={"stock_code": code, "status": job.get("status") or "not_found", "ready": False})


@app.get("/report/{stock_code}", response_class=HTMLResponse)
async def canonical_report(request: Request, stock_code: str, cached_only: bool = False):
    """Canonical report route: redirects to /reports/{id} if exists, else 404."""
    code = _normalize_stock_code(stock_code)
    if not code or not _STOCK_CODE_RE.match(code):
        return templates.TemplateResponse(
            request, "report_error.html",
            {"stock_code": stock_code, "error": "invalid_stock_code"},
            status_code=400,
        )
    db = SessionLocal()
    try:
        try:
            recent_report_failure(db, code)
        except Exception:
            return templates.TemplateResponse(
                request, "report_error.html",
                {"stock_code": code, "error": "recent_report_not_ready"},
                status_code=404,
            )
        cached = _latest_daily_report(db, code)
    finally:
        db.close()
    if cached:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=f"/reports/{cached.report_id}", status_code=302)
    return templates.TemplateResponse(
        request, "report_error.html",
        {"stock_code": code, "error": "recent_report_not_ready"},
        status_code=404,
    )


@app.get("/demo/report/{stock_code}/status")
async def demo_report_status(stock_code: str):
    code = _validate_stock_code(stock_code)
    db = SessionLocal()
    try:
        status_payload = report_status_payload(code, db=db)
    finally:
        db.close()
    return envelope(data=status_payload)



@app.get("/health")
async def health():
    from datetime import datetime, timezone

    db_status = "ok"
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
    except Exception:
        db_status = "degraded"

    scheduler_status = "disabled"
    if settings.enable_scheduler:
        try:
            from app.services import scheduler as scheduler_mod
            sched = getattr(scheduler_mod, "scheduler", None)
            scheduler_status = "ok" if sched and getattr(sched, "running", False) else "degraded"
        except Exception:
            scheduler_status = "degraded"

    llm_router_status = "unconfigured"
    try:
        from app.services.llm_router import get_primary_status
        llm_router_status = get_primary_status()
    except Exception:
        pass

    # Hotspot health: align with internal freshness semantics to avoid false "ok".
    hotspot_status = "ok"
    try:
        db_temp = SessionLocal()
        from app.models import HotspotRaw, MarketHotspotItemSource

        now_utc = datetime.now(timezone.utc)
        last_raw = db_temp.query(HotspotRaw).order_by(HotspotRaw.fetch_time.desc()).first()

        sources_seen: dict[str, str] = {}
        for row in db_temp.query(MarketHotspotItemSource).order_by(MarketHotspotItemSource.fetch_time.desc()).all():
            source_name = str(row.source_name or "")
            if not source_name or source_name in sources_seen:
                continue
            fetch_time = row.fetch_time
            if fetch_time and fetch_time.tzinfo is None:
                fetch_time = fetch_time.replace(tzinfo=timezone.utc)
            if fetch_time is None:
                freshness = "stale"
            else:
                age_hours = (now_utc - fetch_time).total_seconds() / 3600
                freshness = "fresh" if age_hours < 1 else ("degraded" if age_hours < 24 else "stale")
            sources_seen[source_name] = freshness

        if not sources_seen and last_raw is None:
            hotspot_status = "degraded"
        elif any(value != "fresh" for value in sources_seen.values()):
            hotspot_status = "degraded"
        else:
            hotspot_status = "ok"
        db_temp.close()
    except Exception:
        hotspot_status = "degraded"

    # Report chain health
    report_chain_status = "ok"
    try:
        db_temp = SessionLocal()
        from app.models import Report
        from sqlalchemy import func as sa_func

        visible_ok_report_count = (
            db_temp.query(sa_func.count(Report.report_id))
            .filter(Report.published == True, Report.is_deleted == False)  # noqa: E712
            .filter(sa_func.lower(sa_func.coalesce(Report.quality_flag, "ok")) == "ok")
            .scalar()
            or 0
        )
        report_chain_status = "ok" if visible_ok_report_count > 0 else "degraded"
        db_temp.close()
    except Exception:
        report_chain_status = "degraded"

    # Settlement coverage health (角度29: P0指标纳入健康检测)
    settlement_status = "ok"
    settlement_coverage_pct = 0.0
    try:
        db_temp = SessionLocal()
        from app.models import Report as _Report, SettlementResult
        from sqlalchemy import func as sa_func
        visible_ok_count = (
            db_temp.query(sa_func.count(_Report.report_id))
            .filter(_Report.published == True, _Report.is_deleted == False)  # noqa: E712
            .filter(sa_func.lower(sa_func.coalesce(_Report.quality_flag, "ok")) == "ok")
            .filter(_Report.recommendation == "BUY")
            .scalar()
            or 0
        )
        settlement_count = (
            db_temp.query(sa_func.count(sa_func.distinct(SettlementResult.report_id)))
            .join(_Report, _Report.report_id == SettlementResult.report_id)
            .filter(_Report.published == True, _Report.is_deleted == False)  # noqa: E712
            .filter(sa_func.lower(sa_func.coalesce(_Report.quality_flag, "ok")) == "ok")
            .filter(_Report.recommendation == "BUY")
            .scalar()
            or 0
        )
        db_temp.close()
        if visible_ok_count > 0:
            settlement_coverage_pct = round(settlement_count / visible_ok_count * 100, 1)
            settlement_status = "ok" if settlement_count >= visible_ok_count else "degraded"
        else:
            settlement_status = "ok"
    except Exception:
        settlement_status = "degraded"

    # K-line coverage health (角度29: kline覆盖率 < 10% 标记为 degraded)
    kline_status = "ok"
    kline_coverage_pct = 0.0
    try:
        db_temp = SessionLocal()
        from app.models import KlineDaily, StockMaster
        from sqlalchemy import func as sa_func2
        kline_stocks = db_temp.query(sa_func2.count(sa_func2.distinct(KlineDaily.stock_code))).scalar() or 0
        stock_total = db_temp.query(StockMaster).count()
        db_temp.close()
        if stock_total > 0:
            kline_coverage_pct = round(kline_stocks / stock_total * 100, 1)
            kline_status = "ok" if kline_coverage_pct >= 10.0 else "degraded"
        else:
            kline_status = "degraded"
    except Exception:
        kline_status = "degraded"

    overall = "ok"
    if db_status != "ok" or scheduler_status == "degraded":
        overall = "degraded"
    if hotspot_status == "degraded" and report_chain_status == "degraded":
        overall = "degraded"
    if settlement_status == "degraded":
        overall = "degraded"
    if llm_router_status == "unconfigured" and overall == "ok":
        overall = "ok"  # LLM unconfigured is acceptable

    return envelope(data={
        "status": overall,
        "database_status": db_status,
        "scheduler_status": scheduler_status,
        "llm_router_status": llm_router_status,
        "hotspot_status": hotspot_status,
        "report_chain_status": report_chain_status,
        "settlement_status": settlement_status,
        "settlement_coverage_pct": settlement_coverage_pct,
        "kline_status": kline_status,
        "kline_coverage_pct": kline_coverage_pct,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    })


@app.get("/api/v1/health")
async def health_alias_v1():
    return await health()


@app.get("/favicon.ico", include_in_schema=False)
async def favicon_noop():
    return Response(status_code=204)


@app.exception_handler(HTTPException)
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    path = request.url.path
    is_api = path.startswith("/api/") or path.startswith("/auth/") or path.startswith("/billing/")
    if exc.status_code == 400 and not is_api:
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(
            request,
            "400.html",
            {"current_user": user, "detail": humanize_public_error_detail(exc.detail, path=path)},
            status_code=400,
        )
    # 404 且非 API：返回 404 页面
    if exc.status_code == 404 and not is_api:
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(request, "404.html", {"current_user": user}, status_code=404)
    # 401 且非 API：跳转登录
    if exc.status_code == 401 and not is_api:
        from urllib.parse import quote
        next_path = request.url.path
        if request.query_params:
            next_path = f"{next_path}?{request.query_params}"
        return RedirectResponse(url=f"/login?next={quote(next_path)}", status_code=302)
    # 403 且非 API：返回 403 页面
    if exc.status_code == 403 and not is_api:
        user = await get_current_user_optional(request)
        detail = exc.detail
        show_admin_login = user is None  # 仅对匿名用户显示管理员登录入口
        if detail == "REPORT_NOT_AVAILABLE":
            detail = "当前研报暂不在你的可见范围内。"
        return templates.TemplateResponse(
            request,
            "403.html",
            {"current_user": user, "detail": detail, "show_admin_login": show_admin_login},
            status_code=403,
        )
    # 500/503 且非 API：返回错误页面
    if exc.status_code in (500, 503) and not is_api:
        tpl = "500.html"
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(
            request, tpl, {"current_user": user, "detail": exc.detail}, status_code=exc.status_code
        )
    # 400 且非 API：返回 HTML 错误页面
    if exc.status_code == 400 and not is_api:
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(
            request, "400.html", {"current_user": user, "detail": exc.detail}, status_code=400
        )
    canonical = normalize_error_code(exc.detail, status_code=exc.status_code)
    if exc.status_code == 401 and settings.audit_log_enabled:
        logger.warning("auth.denied status_code=401 error_code=%s", canonical)
    resp = envelope(code=exc.status_code, message="error", error=exc.detail, error_code=canonical)
    return fastapi_json_response(
        status_code=exc.status_code,
        content=resp,
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    path = request.url.path
    is_api = path.startswith("/api/")
    if not is_api:
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(request, "400.html", {"current_user": user, "detail": "请求参数无效"}, status_code=422)
    resp = envelope(code=422, message="error", error="INVALID_PAYLOAD", error_code="INVALID_PAYLOAD")
    return fastapi_json_response(status_code=422, content=resp)


@app.exception_handler(Exception)
async def unexpected_exception_handler(request: Request, exc: Exception):
    from sqlalchemy.exc import OperationalError as SAOperationalError
    logger.exception("unhandled_exception: %s", exc)
    path = request.url.path
    is_api = path.startswith("/api/")
    if isinstance(exc, SAOperationalError) and not is_api:
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(request, "503.html", {"current_user": user, "detail": "数据库暂时不可用"}, status_code=503)
    if not is_api:
        user = await get_current_user_optional(request)
        return templates.TemplateResponse(request, "500.html", {"current_user": user}, status_code=500)
    msg = str(exc) if settings.expose_error_details else "internal_error"
    resp = envelope(code=500, message="internal_error", error=msg, error_code="INTERNAL_ERROR")
    return fastapi_json_response(
        status_code=500,
        content=resp,
    )
