from pydantic import BaseModel, Field


class AnalyzeRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=10000, description="Prompt for Qwen")
    timeout_s: int = Field(120, ge=10, le=600, description="Timeout in seconds")


class StockItem(BaseModel):
    code: str
    name: str
    prompt: str = Field(..., min_length=1, max_length=10000)


class BatchRequest(BaseModel):
    stocks: list[StockItem] = Field(..., min_length=1, max_length=5)
    timeout_s: int = Field(120, ge=10, le=600)
