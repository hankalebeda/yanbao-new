from typing import Literal

from pydantic import BaseModel, Field

Provider = Literal["chatgpt", "deepseek", "gemini", "qwen"]


class AnalyzeRequest(BaseModel):
    provider: Provider
    prompt: str = Field(..., min_length=1, max_length=10000)
    timeout_s: int = Field(120, ge=10, le=600)


class StockItem(BaseModel):
    code: str
    name: str
    prompt: str = Field(..., min_length=1, max_length=10000)


class BatchRequest(BaseModel):
    provider: Provider
    stocks: list[StockItem] = Field(..., min_length=1, max_length=5)
    timeout_s: int = Field(120, ge=10, le=600)
