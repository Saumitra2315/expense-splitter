from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class GroupMemberCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    member_id: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)


class GroupCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    base_currency: str = Field(default="USD", min_length=3, max_length=3)
    members: list[GroupMemberCreate] = Field(default_factory=list)

    @field_validator("base_currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()


class MembershipChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["add", "remove"]
    member_id: str = Field(..., min_length=1)
    display_name: str | None = None
    effective_at: datetime | None = None
    expected_version: int | None = Field(default=None, ge=1)
