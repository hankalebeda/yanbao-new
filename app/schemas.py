from typing import Literal

from pydantic import BaseModel, Field
from pydantic.types import StringConstraints
from typing_extensions import Annotated

StockCode = Annotated[str, StringConstraints(pattern=r"^\d{6}\.(SH|SZ)$")]


class GenerateReportRequest(BaseModel):
    stock_code: StockCode = Field(..., examples=["600519.SH"])
    run_mode: Literal["hourly", "daily"] = "daily"
    trade_date: str | None = None  # YYYY-MM-DD，默认当前交易日
    idempotency_key: str | None = None
    source: Literal["real", "test"] = "real"  # test=测试样本，列表默认排除；仅 mock_llm 时允许 test


class HotspotCollectRequest(BaseModel):
    top_n: int = 50


class LLMGenerateRequest(BaseModel):
    prompt: str
    use_prod_model: bool = False


class PredictionSettleRequest(BaseModel):
    report_id: str
    stock_code: StockCode
    windows: list[int] = Field(default_factory=lambda: [1, 7, 14, 30, 60])


class BillingCreateOrderRequest(BaseModel):
    user_id: str
    plan_code: Literal["monthly", "quarterly", "yearly"]
    channel: Literal["mock", "wechat", "alipay"] = "mock"


class BillingCreateOrderV2Request(BaseModel):
    tier_id: str
    period_months: int
    provider: str


class BillingCallbackRequest(BaseModel):
    order_id: str
    paid: bool
    tx_id: str | None = None


class BillingWebhookRequest(BaseModel):
    event_id: str
    order_id: str
    user_id: str
    tier_id: str
    paid_amount: float
    provider: str
    signature: str | None = None


class ReportFeedbackRequest(BaseModel):
    report_id: str
    is_helpful: bool
    feedback_type: Literal["direction", "data", "logic", "other"] | None = None
    comment: str | None = Field(None, max_length=200)
