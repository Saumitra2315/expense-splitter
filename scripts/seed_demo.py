#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from internal.models.budget import BudgetCreate
from internal.models.expense import AllocationInput, ExpenseCreate
from internal.models.fx import FXRateCreate
from internal.models.group import GroupCreate, GroupMemberCreate
from internal.models.notification import NotificationPreferenceCreate
from internal.models.recurring import MaterializeRecurringRequest, RecurringExpenseCreate
from internal.models.settlement import SettlementCreate
from internal.service.budget_service import BudgetService
from internal.service.ledger_service import LedgerService, RequestMetadata
from internal.service.notification_service import NotificationService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed a demo SettleUp dataset")
    parser.add_argument(
        "--db-path",
        help="Path to SQLite DB (defaults to SETTLEUP_DB_PATH or ./settleup.db)",
    )
    parser.add_argument(
        "--group-name",
        default="SettleUp Demo Trip",
        help="Name for the demo group",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.db_path:
        os.environ["SETTLEUP_DB_PATH"] = args.db_path

    ledger = LedgerService()
    budget_service = BudgetService(db_path=ledger.db_path)
    notification_service = NotificationService(db_path=ledger.db_path)
    now = datetime.now(UTC).replace(microsecond=0)
    expense_1_at = now + timedelta(minutes=1)
    expense_2_at = now + timedelta(minutes=2)
    expense_3_at = now + timedelta(minutes=3)
    recurring_start_at = now + timedelta(minutes=4)
    settlement_at = now + timedelta(minutes=5)

    group = ledger.create_group(
        GroupCreate(
            name=args.group_name,
            base_currency="USD",
            members=[
                GroupMemberCreate(member_id="alice", display_name="Alice"),
                GroupMemberCreate(member_id="bob", display_name="Bob"),
                GroupMemberCreate(member_id="cara", display_name="Cara"),
            ],
        ),
        RequestMetadata(idempotency_key=f"seed:{now.isoformat()}:group"),
    )
    group_id = group["id"]

    ledger.create_fx_rate(
        FXRateCreate(
            base_currency="EUR",
            quote_currency="USD",
            rate=Decimal("1.10"),
            effective_at=now,
            source="seed-script",
        ),
        RequestMetadata(idempotency_key=f"seed:{group_id}:fx"),
    )

    ledger.create_expense(
        ExpenseCreate(
            group_id=group_id,
            paid_by="alice",
            amount=Decimal("300.00"),
            currency_code="EUR",
            occurred_at=expense_1_at,
            description="Hotel booking",
            participant_ids=["alice", "bob", "cara"],
            split_mode="equal",
        ),
        RequestMetadata(idempotency_key=f"seed:{group_id}:expense:hotel"),
    )

    ledger.create_expense(
        ExpenseCreate(
            group_id=group_id,
            paid_by="bob",
            amount=Decimal("90.00"),
            currency_code="USD",
            occurred_at=expense_2_at,
            description="Cab and local transport",
            split_mode="fixed",
            allocations=[
                AllocationInput(member_id="alice", value=Decimal("30")),
                AllocationInput(member_id="bob", value=Decimal("20")),
                AllocationInput(member_id="cara", value=Decimal("40")),
            ],
        ),
        RequestMetadata(idempotency_key=f"seed:{group_id}:expense:transport"),
    )

    ledger.create_expense(
        ExpenseCreate(
            group_id=group_id,
            paid_by="cara",
            amount=Decimal("120.00"),
            currency_code="USD",
            occurred_at=expense_3_at,
            description="Dinner and drinks",
            split_mode="percentage",
            allocations=[
                AllocationInput(member_id="alice", value=Decimal("40")),
                AllocationInput(member_id="bob", value=Decimal("30")),
                AllocationInput(member_id="cara", value=Decimal("30")),
            ],
        ),
        RequestMetadata(idempotency_key=f"seed:{group_id}:expense:dinner"),
    )

    recurring = ledger.create_recurring_template(
        RecurringExpenseCreate(
            group_id=group_id,
            paid_by="alice",
            amount=Decimal("45.00"),
            currency_code="USD",
            description="Weekly groceries",
            participant_ids=["alice", "bob", "cara"],
            split_mode="equal",
            cadence_unit="week",
            cadence_count=1,
            start_at=recurring_start_at,
            ends_at=recurring_start_at + timedelta(days=30),
        ),
        RequestMetadata(idempotency_key=f"seed:{group_id}:recurring"),
    )

    materialized = ledger.materialize_recurring(
        recurring["id"],
        MaterializeRecurringRequest(through=recurring_start_at + timedelta(weeks=2)),
        RequestMetadata(idempotency_key=f"seed:{group_id}:materialize"),
    )

    ledger.record_settlement(
        SettlementCreate(
            group_id=group_id,
            paid_by="bob",
            received_by="alice",
            amount=Decimal("25"),
            currency_code="USD",
            occurred_at=settlement_at,
            description="Partial settle-up",
        ),
        RequestMetadata(idempotency_key=f"seed:{group_id}:settlement"),
    )

    budget_service.create_budget(
        BudgetCreate(
            group_id=group_id,
            category="food",
            amount=Decimal("400"),
            currency_code="USD",
            period="monthly",
            alert_thresholds=[50, 80, 100],
        )
    )
    budget_service.create_budget(
        BudgetCreate(
            group_id=group_id,
            category="transport",
            amount=Decimal("250"),
            currency_code="USD",
            period="monthly",
            alert_thresholds=[60, 85, 100],
        )
    )

    notification_service.set_preferences(
        group_id,
        NotificationPreferenceCreate(
            member_id="bob",
            channel="in_app",
            enabled=True,
            digest_frequency="daily",
            event_types=["expense_created", "settlement_recorded", "budget_threshold"],
        ),
    )

    balances = ledger.get_balances(
        group_id,
        settlement_currency="USD",
        valuation_policy="expense_time",
        as_of=settlement_at + timedelta(minutes=1),
    )
    settlement_plan = ledger.get_settlement_plan(
        group_id,
        settlement_currency="USD",
        valuation_policy="expense_time",
        as_of=settlement_at + timedelta(minutes=1),
    )

    output = {
        "db_path": str(ledger.db_path),
        "group_id": group_id,
        "recurring_created_count": len(materialized["created_expenses"]),
        "balances": balances["balances"],
        "transfers": settlement_plan["transfers"],
    }
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
