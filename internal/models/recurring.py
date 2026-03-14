from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from internal.models.expense import AllocationInput


class RecurringExpenseCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(..., min_length=1)
    paid_by: str = Field(..., min_length=1)
    amount: Decimal = Field(..., gt=0)
    currency_code: str = Field(..., min_length=3, max_length=3)
    description: str | None = None
    participant_ids: list[str] = Field(default_factory=list)
    split_mode: Literal["equal", "fixed", "percentage"] = "equal"
    allocations: list[AllocationInput] = Field(default_factory=list)
    cadence_unit: Literal["day", "week", "month"]
    cadence_count: int = Field(..., ge=1, le=365)
    start_at: datetime
    ends_at: datetime | None = None
    expected_version: int | None = Field(default=None, ge=1)

    @field_validator("currency_code")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def validate_shape(self) -> "RecurringExpenseCreate":
        if self.ends_at and self.ends_at < self.start_at:
            raise ValueError("ends_at must be after start_at")
        if self.split_mode == "equal" and not self.participant_ids:
            raise ValueError("participant_ids are required for equal splits")
        if self.split_mode in {"fixed", "percentage"} and not self.allocations:
            raise ValueError("allocations are required for fixed or percentage splits")
        return self


class MaterializeRecurringRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    through: datetime
