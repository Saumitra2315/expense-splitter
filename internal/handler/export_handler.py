"""Export HTTP handlers for the SettleUp API.

Exposes endpoints for creating and retrieving data exports.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from internal.middleware.auth import verify_token
from internal.models.export import ExportRequest
from internal.service.export_service import ExportService

router = APIRouter(
    tags=["exports"],
    dependencies=[Depends(verify_token)],
)

service = ExportService()


@router.post("/groups/{group_id}/exports", status_code=201)
def create_export(
    group_id: str,
    payload: ExportRequest,
) -> dict[str, Any]:
    """Create a new export job and generate the export data."""
    if payload.group_id != group_id:
        from internal.service.ledger_service import ValidationError
        raise ValidationError("group_id in path must match group_id in body")
    return service.create_export(payload)


@router.get("/groups/{group_id}/exports", status_code=200)
def list_exports(
    group_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """List all export jobs for a group."""
    return service.list_exports(group_id, limit=limit, offset=offset)


@router.get("/groups/{group_id}/exports/{export_id}", status_code=200)
def get_export(
    group_id: str,
    export_id: str,
) -> dict[str, Any]:
    """Get export job status and result."""
    return service.get_export(group_id, export_id)
