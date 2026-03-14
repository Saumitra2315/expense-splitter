"""Notification models for the Expense Splitter API.

Defines Pydantic schemas for notification preferences, events,
and digest configuration.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


NotificationChannel = Literal["in_app", "email", "push"]
NotificationEventType = Literal[
    "expense_created",
    "expense_updated",
    "settlement_recorded",
    "member_added",
    "member_removed",
    "budget_threshold",
    "balance_reminder",
    "group_created",
    "recurring_materialized",
]
DigestFrequency = Literal["daily", "weekly", "monthly", "none"]


class NotificationPreferenceCreate(BaseModel):
    """Schema for creating or updating notification preferences."""

    model_config = ConfigDict(extra="forbid")

    member_id: str = Field(..., min_length=1, description="The member whose preferences to set")
    channel: NotificationChannel = Field(
        default="in_app",
        description="Delivery channel for notifications",
    )
    enabled: bool = Field(default=True, description="Whether notifications are enabled")
    event_types: list[NotificationEventType] = Field(
        default_factory=lambda: [
            "expense_created",
            "settlement_recorded",
            "member_added",
            "member_removed",
        ],
        description="Event types to receive notifications for",
    )
    threshold_amount: Decimal | None = Field(
        default=None,
        ge=0,
        description="Amount threshold — only notify if expense exceeds this",
    )
    quiet_hours_start: int | None = Field(
        default=None,
        ge=0,
        le=23,
        description="Start of quiet hours (hour in UTC, 0-23)",
    )
    quiet_hours_end: int | None = Field(
        default=None,
        ge=0,
        le=23,
        description="End of quiet hours (hour in UTC, 0-23)",
    )
    digest_frequency: DigestFrequency = Field(
        default="none",
        description="How often to send digest summaries",
    )


class NotificationPreferenceUpdate(BaseModel):
    """Schema for partial updates to notification preferences."""

    model_config = ConfigDict(extra="forbid")

    channel: NotificationChannel | None = None
    enabled: bool | None = None
    event_types: list[NotificationEventType] | None = None
    threshold_amount: Decimal | None = None
    quiet_hours_start: int | None = Field(default=None, ge=0, le=23)
    quiet_hours_end: int | None = Field(default=None, ge=0, le=23)
    digest_frequency: DigestFrequency | None = None


class NotificationEventCreate(BaseModel):
    """Schema for creating a notification event (internal use)."""

    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(..., min_length=1)
    event_type: NotificationEventType
    triggered_by: str = Field(..., min_length=1, description="Member who caused the event")
    payload: dict[str, Any] = Field(default_factory=dict)
    amount: Decimal | None = Field(default=None, ge=0)
    currency_code: str | None = Field(default=None, min_length=3, max_length=3)

    @field_validator("currency_code")
    @classmethod
    def normalize_currency(cls, value: str | None) -> str | None:
        return value.upper() if value else None


class NotificationDigestRequest(BaseModel):
    """Schema for requesting a notification digest."""

    model_config = ConfigDict(extra="forbid")

    since: datetime | None = Field(
        default=None,
        description="Start of digest period (defaults to last digest)",
    )
    through: datetime | None = Field(
        default=None,
        description="End of digest period (defaults to now)",
    )
    include_read: bool = Field(
        default=False,
        description="Whether to include already-read events",
    )


class NotificationMarkReadRequest(BaseModel):
    """Schema for marking notifications as read."""

    model_config = ConfigDict(extra="forbid")

    event_ids: list[str] = Field(
        ...,
        min_length=1,
        description="IDs of notification events to mark as read",
    )
