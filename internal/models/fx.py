from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FXRateCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_currency: str = Field(..., min_length=3, max_length=3)
    quote_currency: str = Field(..., min_length=3, max_length=3)
    rate: Decimal = Field(..., gt=0)
    effective_at: datetime | None = None
    source: str | None = None

    @field_validator("base_currency", "quote_currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()
