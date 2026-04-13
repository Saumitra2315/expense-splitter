from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from internal.models.budget import BudgetCreate
from internal.models.expense import ExpenseCreate
from internal.models.group import GroupCreate, GroupMemberCreate
from internal.service.ledger_service import RequestMetadata


def test_budget_summary_tracks_spend_for_matching_category(ledger_service, budget_service):
    group = ledger_service.create_group(
        GroupCreate(
            name="Trip",
            base_currency="USD",
            members=[
                GroupMemberCreate(member_id="alice", display_name="Alice"),
                GroupMemberCreate(member_id="bob", display_name="Bob"),
            ],
        ),
        RequestMetadata(),
    )
    group_id = group["id"]

    budget_service.create_budget(
        BudgetCreate(
            group_id=group_id,
            category="food",
            amount=Decimal("100"),
            currency_code="USD",
            period="monthly",
            alert_thresholds=[50, 80, 100],
        )
    )

    now = datetime.now(UTC)
    ledger_service.create_expense(
        ExpenseCreate(
            group_id=group_id,
            paid_by="alice",
            amount=Decimal("30"),
            currency_code="USD",
            occurred_at=now,
            description="restaurant dinner",
            participant_ids=["alice", "bob"],
            split_mode="equal",
        ),
        RequestMetadata(),
    )
    ledger_service.create_expense(
        ExpenseCreate(
            group_id=group_id,
            paid_by="alice",
            amount=Decimal("25"),
            currency_code="USD",
            occurred_at=now,
            description="taxi ride",
            participant_ids=["alice", "bob"],
            split_mode="equal",
        ),
        RequestMetadata(),
    )

    summary = budget_service.get_budget_summary(group_id)
    assert summary["total_budget"] == "100"
    assert summary["total_spent"] == "30.00"
    assert summary["total_remaining"] == "70.00"
    assert summary["overall_utilization_pct"] == 30.0
    assert summary["categories"][0]["category"] == "food"
    assert summary["categories"][0]["status"] == "healthy"
