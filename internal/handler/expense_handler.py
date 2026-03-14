from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status

from internal.middleware.auth import verify_token
from internal.models.expense import Expense, ExpenseCreate
from internal.storage.inmemory import balances, expenses, groups

router = APIRouter(
    prefix="/expenses",
    tags=["expenses"],
    dependencies=[Depends(verify_token)],
)


@router.post("", response_model=Expense)
def add_expense(payload: ExpenseCreate) -> Expense:
    if payload.group_id not in groups:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group not found",
        )

    expense = Expense(
        id=str(uuid4()),
        group_id=payload.group_id,
        paid_by=payload.paid_by,
        amount=payload.amount,
        split_between=payload.split_between,
        description=payload.description,
    )
    expenses[expense.id] = expense
    group_balances = balances.setdefault(payload.group_id, {})
    group_balances[payload.paid_by] = group_balances.get(payload.paid_by, 0.0) + payload.amount
    return expense
