from typing import Any

from fastapi import APIRouter, Depends, Header, Query

from internal.middleware.auth import verify_token
from internal.models.group import GroupCreate, MembershipChange
from internal.service.ledger_service import LedgerService, RequestMetadata

router = APIRouter(
    prefix="/groups",
    tags=["groups"],
    dependencies=[Depends(verify_token)],
)

service = LedgerService()


@router.post("", status_code=201)
def create_group(
    payload: GroupCreate,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.create_group(payload, metadata)


@router.get("/{group_id}")
def get_group(group_id: str) -> dict[str, Any]:
    return service.get_group(group_id)


@router.post("/{group_id}/members")
def change_membership(
    group_id: str,
    payload: MembershipChange,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.change_membership(group_id, payload, metadata)
