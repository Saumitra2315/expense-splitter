from uuid import uuid4

from fastapi import APIRouter, Depends

from internal.middleware.auth import verify_token
from internal.models.group import Group, GroupCreate
from internal.storage.inmemory import balances, groups

router = APIRouter(
    prefix="/groups",
    tags=["groups"],
    dependencies=[Depends(verify_token)],
)


@router.post("", response_model=Group)
def create_group(payload: GroupCreate) -> Group:
    group = Group(id=str(uuid4()), name=payload.name, members=payload.members)
    groups[group.id] = group
    balances[group.id] = {member: 0.0 for member in group.members}
    return group
