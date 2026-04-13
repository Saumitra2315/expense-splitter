from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from internal.models.expense import AllocationInput, ExpenseCreate
from internal.models.fx import FXRateCreate
from internal.models.group import GroupCreate, GroupMemberCreate
from internal.models.recurring import MaterializeRecurringRequest, RecurringExpenseCreate
from internal.service.ledger_service import RequestMetadata
from internal.service.split_service import compute_allocations


def _create_group(service, member_ids: list[str], base_currency: str = "USD") -> dict[str, str]:
    group = service.create_group(
        GroupCreate(
            name="Trip",
            base_currency=base_currency,
            members=[
                GroupMemberCreate(member_id=member_id, display_name=member_id.title())
                for member_id in member_ids
            ],
        ),
        RequestMetadata(),
    )
    return {"group_id": group["id"]}


def _balances_map(balances_response: dict) -> dict[str, Decimal]:
    return {
        item["member_id"]: Decimal(item["net_amount"])
        for item in balances_response["balances"]
    }


def test_group_creation_returns_members(ledger_service):
    response = ledger_service.create_group(
        GroupCreate(
            name="Goa Trip",
            base_currency="USD",
            members=[
                GroupMemberCreate(member_id="alice", display_name="Alice"),
                GroupMemberCreate(member_id="bob", display_name="Bob"),
            ],
        ),
        RequestMetadata(),
    )

    assert response["name"] == "Goa Trip"
    assert response["version"] == 1
    assert {member["member_id"] for member in response["members"]} == {"alice", "bob"}


def test_equal_split_allocation(ledger_service):
    ids = _create_group(ledger_service, ["alice", "bob", "cara"])
    expense = ledger_service.create_expense(
        ExpenseCreate(
            group_id=ids["group_id"],
            paid_by="alice",
            amount=Decimal("100.00"),
            currency_code="USD",
            description="Dinner",
            participant_ids=["alice", "bob", "cara"],
            split_mode="equal",
        ),
        RequestMetadata(),
    )

    assert expense["allocations"] == {
        "alice": "33.34",
        "bob": "33.33",
        "cara": "33.33",
    }


def test_fixed_split_allocation(ledger_service):
    ids = _create_group(ledger_service, ["alice", "bob", "cara"])
    expense = ledger_service.create_expense(
        ExpenseCreate(
            group_id=ids["group_id"],
            paid_by="alice",
            amount=Decimal("90"),
            currency_code="USD",
            description="Tickets",
            split_mode="fixed",
            allocations=[
                AllocationInput(member_id="alice", value=Decimal("30")),
                AllocationInput(member_id="bob", value=Decimal("20")),
                AllocationInput(member_id="cara", value=Decimal("40")),
            ],
        ),
        RequestMetadata(),
    )

    assert expense["allocations"] == {
        "alice": "30.00",
        "bob": "20.00",
        "cara": "40.00",
    }


def test_percentage_split_allocation(ledger_service):
    ids = _create_group(ledger_service, ["alice", "bob", "cara"])
    expense = ledger_service.create_expense(
        ExpenseCreate(
            group_id=ids["group_id"],
            paid_by="alice",
            amount=Decimal("120"),
            currency_code="USD",
            description="Stay",
            split_mode="percentage",
            allocations=[
                AllocationInput(member_id="alice", value=Decimal("50")),
                AllocationInput(member_id="bob", value=Decimal("30")),
                AllocationInput(member_id="cara", value=Decimal("20")),
            ],
        ),
        RequestMetadata(),
    )

    assert expense["allocations"] == {
        "alice": "60.00",
        "bob": "36.00",
        "cara": "24.00",
    }


def test_rounding_edges_for_currency_scale():
    jpy_allocations = compute_allocations(
        total=Decimal("100"),
        currency_code="JPY",
        split_mode="equal",
        participant_ids=["alice", "bob", "cara"],
        allocations=[],
    )
    bhd_allocations = compute_allocations(
        total=Decimal("1.000"),
        currency_code="BHD",
        split_mode="equal",
        participant_ids=["alice", "bob", "cara"],
        allocations=[],
    )

    assert jpy_allocations == {
        "alice": Decimal("34"),
        "bob": Decimal("33"),
        "cara": Decimal("33"),
    }
    assert bhd_allocations == {
        "alice": Decimal("0.334"),
        "bob": Decimal("0.333"),
        "cara": Decimal("0.333"),
    }


def test_fx_conversion_applies_rate_as_of_expense_time(ledger_service):
    ids = _create_group(ledger_service, ["alice", "bob"], base_currency="USD")
    now = datetime.now(UTC).replace(microsecond=0)
    rate_time = now + timedelta(minutes=1)
    expense_time = now + timedelta(minutes=2)

    ledger_service.create_fx_rate(
        FXRateCreate(
            base_currency="EUR",
            quote_currency="USD",
            rate=Decimal("1.2"),
            effective_at=rate_time,
            source="test",
        ),
        RequestMetadata(),
    )
    ledger_service.create_expense(
        ExpenseCreate(
            group_id=ids["group_id"],
            paid_by="alice",
            amount=Decimal("10"),
            currency_code="EUR",
            description="Museum",
            occurred_at=expense_time,
            participant_ids=["alice", "bob"],
            split_mode="equal",
        ),
        RequestMetadata(),
    )

    balances = ledger_service.get_balances(
        ids["group_id"],
        settlement_currency="USD",
        valuation_policy="expense_time",
        as_of=expense_time,
    )
    net = _balances_map(balances)
    assert net["alice"] == Decimal("6.00")
    assert net["bob"] == Decimal("-6.00")


def test_settlement_plan_regression_routes_to_final_creditor(ledger_service):
    ids = _create_group(ledger_service, ["alice", "bob", "cara"])
    now = datetime.now(UTC).replace(microsecond=0)
    expense_time = now + timedelta(minutes=1)
    ledger_service.create_expense(
        ExpenseCreate(
            group_id=ids["group_id"],
            paid_by="alice",
            amount=Decimal("90"),
            currency_code="USD",
            occurred_at=expense_time,
            description="Hotel",
            participant_ids=["alice", "bob", "cara"],
            split_mode="equal",
        ),
        RequestMetadata(),
    )

    plan = ledger_service.get_settlement_plan(
        ids["group_id"],
        settlement_currency="USD",
        valuation_policy="expense_time",
        as_of=now + timedelta(minutes=2),
    )

    transfers = {
        (item["from_member_id"], item["to_member_id"], item["amount"])
        for item in plan["transfers"]
    }
    assert transfers == {
        ("bob", "alice", "30"),
        ("cara", "alice", "30"),
    }


def test_recurring_materialization_creates_expected_occurrences(ledger_service):
    ids = _create_group(ledger_service, ["alice", "bob"])
    start_at = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=1)

    template = ledger_service.create_recurring_template(
        RecurringExpenseCreate(
            group_id=ids["group_id"],
            paid_by="alice",
            amount=Decimal("40"),
            currency_code="USD",
            description="Weekly groceries",
            participant_ids=["alice", "bob"],
            split_mode="equal",
            cadence_unit="week",
            cadence_count=1,
            start_at=start_at,
        ),
        RequestMetadata(),
    )

    result = ledger_service.materialize_recurring(
        template["id"],
        MaterializeRecurringRequest(through=start_at + timedelta(weeks=2)),
        RequestMetadata(),
    )

    assert len(result["created_expenses"]) == 3
    assert result["next_run_at"] == (start_at + timedelta(weeks=3)).isoformat().replace("+00:00", "Z")
