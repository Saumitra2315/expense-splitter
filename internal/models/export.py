"""Export models for the Expense Splitter API.

Defines Pydantic schemas for export requests, jobs, and result formats.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


ExportFormat = Literal["csv", "json"]
ExportStatus = Literal["pending", "processing", "completed", "failed"]


class ExportFilter(BaseModel):
    """Filters to apply when generating an export."""

    model_config = ConfigDict(extra="forbid")

    member_ids: list[str] | None = Field(
        default=None,
        description="Filter expenses by these member IDs (payer or participant)",
    )
    categories: list[str] | None = Field(
        default=None,
        description="Filter expenses by these categories",
    )
    min_amount: str | None = Field(
        default=None,
        description="Minimum expense amount (inclusive)",
    )
    max_amount: str | None = Field(
        default=None,
        description="Maximum expense amount (inclusive)",
    )
    currency_codes: list[str] | None = Field(
        default=None,
        description="Filter by these currency codes",
    )
    split_modes: list[str] | None = Field(
        default=None,
        description="Filter by split mode (equal, fixed, percentage)",
    )
    include_settlements: bool = Field(
        default=True,
        description="Whether to include settlement records in export",
    )
    include_recurring: bool = Field(
        default=True,
        description="Whether to include recurring expense instances",
    )


class ExportRequest(BaseModel):
    """Schema for creating an export job."""

    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(..., min_length=1)
    format: ExportFormat = Field(default="csv")
    start_date: datetime | None = Field(
        default=None,
        description="Start of date range (inclusive)",
    )
    end_date: datetime | None = Field(
        default=None,
        description="End of date range (inclusive)",
    )
    filters: ExportFilter = Field(default_factory=ExportFilter)
    columns: list[str] | None = Field(
        default=None,
        description="Specific columns to include (default: all)",
    )
    include_summary: bool = Field(
        default=True,
        description="Whether to append summary rows at the end",
    )
    timezone_offset: int = Field(
        default=0,
        ge=-720,
        le=840,
        description="Timezone offset in minutes from UTC for date formatting",
    )

    @model_validator(mode="after")
    def validate_date_range(self) -> "ExportRequest":
        if self.start_date and self.end_date and self.start_date >= self.end_date:
            raise ValueError("start_date must be before end_date")
        return self


# All valid column names for export.
VALID_EXPORT_COLUMNS: frozenset[str] = frozenset({
    "date",
    "type",
    "description",
    "paid_by",
    "amount",
    "currency",
    "split_mode",
    "participants",
    "category",
    "received_by",
    "settlement_amount",
    "recurring_template_id",
    "expense_id",
    "group_name",
})

DEFAULT_EXPORT_COLUMNS: list[str] = [
    "date",
    "type",
    "description",
    "paid_by",
    "amount",
    "currency",
    "split_mode",
    "participants",
]
