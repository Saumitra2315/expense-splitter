"""Notification HTTP handlers for the SettleUp API.

Exposes endpoints for managing notification preferences, listing events,
marking events as read, and computing notification digests.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from internal.middleware.auth import verify_token
from internal.models.notification import (
    NotificationDigestRequest,
    NotificationMarkReadRequest,
    NotificationPreferenceCreate,
    NotificationPreferenceUpdate,
)
from internal.service.notification_service import NotificationService

router = APIRouter(
    prefix="/groups/{group_id}/members/{member_id}/notifications",
    tags=["notifications"],
    dependencies=[Depends(verify_token)],
)

service = NotificationService()


@router.put("", status_code=200)
def set_preferences(
    group_id: str,
    member_id: str,
    payload: NotificationPreferenceCreate,
) -> dict[str, Any]:
    """Create or replace notification preferences for a member."""
    if payload.member_id != member_id:
        from internal.service.ledger_service import ValidationError
        raise ValidationError("member_id in path must match member_id in body")
    return service.set_preferences(group_id, payload)


@router.get("", status_code=200)
def get_preferences(
    group_id: str,
    member_id: str,
) -> dict[str, Any]:
    """Get notification preferences for a member."""
    return service.get_preferences(group_id, member_id)


@router.patch("", status_code=200)
def update_preferences(
    group_id: str,
    member_id: str,
    payload: NotificationPreferenceUpdate,
) -> dict[str, Any]:
    """Partially update notification preferences."""
    return service.update_preferences(group_id, member_id, payload)


@router.get("/events", status_code=200)
def list_events(
    group_id: str,
    member_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    unread_only: bool = Query(False),
) -> dict[str, Any]:
    """List notification events for a member."""
    return service.get_events(
        group_id, member_id, limit=limit, offset=offset, unread_only=unread_only
    )


@router.post("/events/mark-read", status_code=200)
def mark_events_read(
    group_id: str,
    member_id: str,
    payload: NotificationMarkReadRequest,
) -> dict[str, Any]:
    """Mark specific notification events as read."""
    return service.mark_events_read(group_id, member_id, payload.event_ids)


@router.post("/digest", status_code=200)
def compute_digest(
    group_id: str,
    member_id: str,
    payload: NotificationDigestRequest,
) -> dict[str, Any]:
    """Compute a notification digest summary."""
    return service.compute_digest(group_id, member_id, payload)
