from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class OfflineOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_operation_id: str = Field(..., min_length=1)
    operation_type: Literal[
        "membership_change",
        "add_expense",
        "record_settlement",
        "create_recurring_template",
        "materialize_recurring",
    ]
    group_id: str | None = None
    expected_group_version: int | None = Field(default=None, ge=1)
    payload: dict[str, Any] = Field(default_factory=dict)


class SyncRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(..., min_length=1)
    operations: list[OfflineOperation] = Field(default_factory=list)
