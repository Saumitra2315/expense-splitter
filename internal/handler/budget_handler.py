"""Budget HTTP handlers for the Expense Splitter API.

Exposes endpoints for budget management, spending tracking,
and category rule configuration.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from internal.middleware.auth import verify_token
from internal.models.budget import BudgetCreate, BudgetUpdate, CategoryRuleSet
from internal.service.budget_service import BudgetService

router = APIRouter(
    tags=["budgets"],
    dependencies=[Depends(verify_token)],
)

service = BudgetService()


@router.post("/groups/{group_id}/budgets", status_code=201)
def create_budget(
    group_id: str,
    payload: BudgetCreate,
) -> dict[str, Any]:
    """Create a new budget for a group and category."""
    if payload.group_id != group_id:
        from internal.service.ledger_service import ValidationError
        raise ValidationError("group_id in path must match group_id in body")
    return service.create_budget(payload)


@router.get("/groups/{group_id}/budgets", status_code=200)
def list_budgets(
    group_id: str,
    include_inactive: bool = Query(False),
) -> dict[str, Any]:
    """List all budgets for a group with current spending."""
    return service.list_budgets(group_id, include_inactive=include_inactive)


@router.get("/groups/{group_id}/budgets/{budget_id}", status_code=200)
def get_budget(
    group_id: str,
    budget_id: str,
) -> dict[str, Any]:
    """Get budget detail with current spending."""
    return service.get_budget(group_id, budget_id)


@router.patch("/groups/{group_id}/budgets/{budget_id}", status_code=200)
def update_budget(
    group_id: str,
    budget_id: str,
    payload: BudgetUpdate,
) -> dict[str, Any]:
    """Update an existing budget."""
    return service.update_budget(group_id, budget_id, payload)


@router.delete("/groups/{group_id}/budgets/{budget_id}", status_code=200)
def delete_budget(
    group_id: str,
    budget_id: str,
) -> dict[str, Any]:
    """Soft-delete a budget."""
    return service.delete_budget(group_id, budget_id)


@router.get("/groups/{group_id}/budget-summary", status_code=200)
def get_budget_summary(
    group_id: str,
) -> dict[str, Any]:
    """Get an aggregated budget summary across all active budgets."""
    return service.get_budget_summary(group_id)


@router.put("/groups/{group_id}/category-rules", status_code=200)
def set_category_rules(
    group_id: str,
    payload: CategoryRuleSet,
) -> dict[str, Any]:
    """Set or replace category rules for a group."""
    if payload.group_id != group_id:
        from internal.service.ledger_service import ValidationError
        raise ValidationError("group_id in path must match group_id in body")
    return service.set_category_rules(payload)


@router.get("/groups/{group_id}/category-rules", status_code=200)
def get_category_rules(
    group_id: str,
) -> dict[str, Any]:
    """Get category rules for a group."""
    return service.get_category_rules(group_id)
