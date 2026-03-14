from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SettlementCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(..., min_length=1)
    paid_by: str = Field(..., min_length=1)
    received_by: str = Field(..., min_length=1)
    amount: Decimal = Field(..., gt=0)
    currency_code: str = Field(..., min_length=3, max_length=3)
    occurred_at: datetime | None = None
    description: str | None = None
    expected_version: int | None = Field(default=None, ge=1)

    @field_validator("currency_code")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()
