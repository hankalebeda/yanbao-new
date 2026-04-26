from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.error_codes import INTERNAL_ERROR
from app.core.response import envelope
from app.core.security import get_current_user_optional
from app.schemas import BillingCreateOrderV2Request, BillingWebhookRequest
from app.services.membership import (
    TierAlreadyActiveError,
    create_order,
    handle_webhook,
    serialize_order,
    verify_webhook_signature,
)

router = APIRouter(tags=["billing"])


@router.post("/billing/create_order", status_code=201)
async def billing_create_order(
    payload: BillingCreateOrderV2Request,
    request: Request,
    db: Session = Depends(get_db),
):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    try:
        order = create_order(
            db,
            user=user,
            tier_id=payload.tier_id,
            period_months=payload.period_months,
            provider=payload.provider,
        )
    except TierAlreadyActiveError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="TIER_ALREADY_ACTIVE") from exc
    except ValueError as exc:
        db.rollback()
        exc_msg = str(exc)
        if exc_msg == "VALIDATION_FAILED":
            raise HTTPException(status_code=422, detail="VALIDATION_FAILED") from exc
        if exc_msg.startswith("PAYMENT_PROVIDER_NOT_CONFIGURED"):
            raise HTTPException(status_code=503, detail="PAYMENT_PROVIDER_NOT_CONFIGURED") from exc
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD") from exc
    db.commit()
    db.refresh(order)
    return envelope(data=serialize_order(order))


@router.post("/billing/webhook")
async def billing_webhook(
    payload: BillingWebhookRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    header_signature = request.headers.get("Webhook-Signature") or request.headers.get("X-Signature")
    if not verify_webhook_signature(
        header_signature,
        payload.signature,
        payload.event_id,
        payload.order_id,
        payload.user_id,
        payload.tier_id,
        payload.paid_amount,
        payload.provider,
    ):
        raise HTTPException(status_code=400, detail="PAYMENT_SIGNATURE_INVALID")
    try:
        result = handle_webhook(
            db,
            event_id=payload.event_id,
            order_id=payload.order_id,
            user_id=payload.user_id,
            tier_id=payload.tier_id,
            paid_amount=payload.paid_amount,
            provider=payload.provider,
            payload=payload.model_dump(),
        )
    except ValueError as exc:
        db.rollback()
        if str(exc) == "VALIDATION_FAILED":
            raise HTTPException(status_code=422, detail="INVALID_PAYLOAD") from exc
        raise HTTPException(status_code=500, detail=INTERNAL_ERROR) from exc
    db.commit()
    return envelope(data=result)


@router.get("/billing/mock-pay/{order_id}", response_class=HTMLResponse)
async def billing_mock_pay_page(order_id: str, request: Request, db: Session = Depends(get_db)):
    del order_id, request, db
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


@router.post("/billing/mock-pay/{order_id}/confirm")
async def billing_mock_pay_confirm(order_id: str, request: Request, db: Session = Depends(get_db)):
    del order_id, request, db
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")
