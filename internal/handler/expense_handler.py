from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, Query

from internal.middleware.auth import verify_token
from internal.models.expense import ExpenseCreate
from internal.models.fx import FXRateCreate
from internal.models.recurring import MaterializeRecurringRequest, RecurringExpenseCreate
from internal.models.settlement import SettlementCreate
from internal.models.sync import SyncRequest
from internal.service.ledger_service import LedgerService, RequestMetadata

router = APIRouter(
    dependencies=[Depends(verify_token)],
)

service = LedgerService()


@router.post("/expenses", status_code=201, tags=["expenses"])
def create_expense(
    payload: ExpenseCreate,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.create_expense(payload, metadata)


@router.post("/fx-rates", status_code=201, tags=["fx"])
def create_fx_rate(
    payload: FXRateCreate,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.create_fx_rate(payload, metadata)


@router.post("/recurring-templates", status_code=201, tags=["recurring"])
def create_recurring_template(
    payload: RecurringExpenseCreate,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.create_recurring_template(payload, metadata)


@router.post("/recurring-templates/{template_id}/materialize", tags=["recurring"])
def materialize_recurring(
    template_id: str,
    payload: MaterializeRecurringRequest,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.materialize_recurring(template_id, payload, metadata)


@router.post("/settlements", status_code=201, tags=["settlements"])
def record_settlement(
    payload: SettlementCreate,
    idempotency_key: str | None = Header(None),
) -> dict[str, Any]:
    metadata = RequestMetadata(idempotency_key=idempotency_key)
    return service.record_settlement(payload, metadata)


@router.get("/groups/{group_id}/balances", tags=["balances"])
def get_balances(
    group_id: str,
    settlement_currency: str | None = Query(None),
    valuation_policy: str = Query("expense_time"),
    as_of: datetime | None = Query(None),
) -> dict[str, Any]:
    return service.get_balances(
        group_id,
        settlement_currency=settlement_currency,
        valuation_policy=valuation_policy,
        as_of=as_of,
    )


@router.get("/groups/{group_id}/settlement-plan", tags=["balances"])
def get_settlement_plan(
    group_id: str,
    settlement_currency: str | None = Query(None),
    valuation_policy: str = Query("expense_time"),
    as_of: datetime | None = Query(None),
) -> dict[str, Any]:
    return service.get_settlement_plan(
        group_id,
        settlement_currency=settlement_currency,
        valuation_policy=valuation_policy,
        as_of=as_of,
    )


@router.get("/groups/{group_id}/audit", tags=["audit"])
def get_audit_history(
    group_id: str,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    return service.get_audit_history(group_id, limit)


@router.post("/sync", tags=["sync"])
def sync_offline(payload: SyncRequest) -> dict[str, Any]:
    return service.sync(payload)
