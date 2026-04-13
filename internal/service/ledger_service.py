from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from internal.models.expense import ExpenseCreate
from internal.models.fx import FXRateCreate
from internal.models.group import GroupCreate, MembershipChange
from internal.models.recurring import MaterializeRecurringRequest, RecurringExpenseCreate
from internal.models.settlement import SettlementCreate
from internal.models.sync import SyncRequest
from internal.service.ledger_common import (
    advance_time,
    coerce_time,
    decimal_text,
    iso,
    json_dumps,
    now_utc,
    parse_time,
    request_hash,
)
from internal.service.split_service import build_transfer_plan, compute_allocations, quantize_money
from internal.storage.sqlite import init_db, read_connection, resolve_db_path, write_connection


class ServiceError(Exception):
    status_code = 400


class NotFoundError(ServiceError):
    status_code = 404


class ConflictError(ServiceError):
    status_code = 409


class ValidationError(ServiceError):
    status_code = 422


@dataclass(frozen=True)
class RequestMetadata:
    idempotency_key: str | None = None
    source: str = "http"
    device_id: str | None = None
    client_operation_id: str | None = None


class LedgerService:
    def __init__(self) -> None:
        self.db_path = init_db(resolve_db_path())

    def create_group(self, payload: GroupCreate, metadata: RequestMetadata) -> dict[str, Any]:
        request_data = payload.model_dump(mode="json")
        with write_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(conn, self._idempotency_scope("group.create", metadata), request_data)
            if replay is not None:
                return replay

            group_id = str(uuid4())
            created_at = now_utc()
            conn.execute(
                """
                INSERT INTO groups(id, name, base_currency, version, created_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (group_id, payload.name, payload.base_currency, 1, iso(created_at)),
            )

            members = []
            for member in payload.members:
                conn.execute(
                    """
                    INSERT INTO group_members(group_id, member_id, display_name, active, joined_at, left_at)
                    VALUES(?, ?, ?, 1, ?, NULL)
                    """,
                    (group_id, member.member_id, member.display_name, iso(created_at)),
                )
                conn.execute(
                    """
                    INSERT INTO membership_events(id, group_id, member_id, action, display_name, effective_at, version, created_at)
                    VALUES(?, ?, ?, 'add', ?, ?, ?, ?)
                    """,
                    (str(uuid4()), group_id, member.member_id, member.display_name, iso(created_at), 1, iso(created_at)),
                )
                members.append(
                    {
                        "member_id": member.member_id,
                        "display_name": member.display_name,
                        "active": True,
                        "joined_at": iso(created_at),
                        "left_at": None,
                    }
                )

            response = {
                "id": group_id,
                "name": payload.name,
                "base_currency": payload.base_currency,
                "version": 1,
                "members": members,
            }
            self._record_audit(
                conn,
                group_id=group_id,
                event_type="group_created",
                entity_type="group",
                entity_id=group_id,
                payload=response,
                group_version=1,
                occurred_at=created_at,
            )
            self._store_idempotent_response(conn, self._idempotency_scope("group.create", metadata), request_data, response)
            return response

    def get_group(self, group_id: str) -> dict[str, Any]:
        with read_connection(self.db_path) as conn:
            group = self._get_group_row(conn, group_id)
            members = self._current_members(conn, group_id)
            return {
                "id": group["id"],
                "name": group["name"],
                "base_currency": group["base_currency"],
                "version": group["version"],
                "members": members,
            }

    def change_membership(
        self,
        group_id: str,
        payload: MembershipChange,
        metadata: RequestMetadata,
    ) -> dict[str, Any]:
        request_data = {"group_id": group_id, **payload.model_dump(mode="json")}
        with write_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(
                conn,
                self._idempotency_scope(f"group.membership.{group_id}", metadata),
                request_data,
            )
            if replay is not None:
                return replay

            group = self._get_group_row(conn, group_id)
            if payload.expected_version and group["version"] != payload.expected_version:
                raise ConflictError(f"Expected group version {payload.expected_version}, found {group['version']}")

            effective_at = coerce_time(payload.effective_at)
            snapshot = {member["member_id"]: member for member in self._current_members(conn, group_id)}

            if payload.action == "add":
                if payload.member_id in snapshot and snapshot[payload.member_id]["active"]:
                    raise ConflictError("Member is already active")
                if not payload.display_name:
                    raise ValidationError("display_name is required when adding a member")
                conn.execute(
                    """
                    INSERT INTO group_members(group_id, member_id, display_name, active, joined_at, left_at)
                    VALUES(?, ?, ?, 1, ?, NULL)
                    ON CONFLICT(group_id, member_id) DO UPDATE SET
                        display_name = excluded.display_name,
                        active = 1,
                        joined_at = excluded.joined_at,
                        left_at = NULL
                    """,
                    (group_id, payload.member_id, payload.display_name, iso(effective_at)),
                )
            else:
                if payload.member_id not in snapshot or not snapshot[payload.member_id]["active"]:
                    raise ConflictError("Member is not currently active")
                conn.execute(
                    """
                    UPDATE group_members
                    SET active = 0, left_at = ?
                    WHERE group_id = ? AND member_id = ?
                    """,
                    (iso(effective_at), group_id, payload.member_id),
                )

            version = self._bump_group_version(conn, group_id)
            conn.execute(
                """
                INSERT INTO membership_events(id, group_id, member_id, action, display_name, effective_at, version, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    group_id,
                    payload.member_id,
                    payload.action,
                    payload.display_name,
                    iso(effective_at),
                    version,
                    iso(now_utc()),
                ),
            )

            response = {
                "group_id": group_id,
                "member_id": payload.member_id,
                "action": payload.action,
                "effective_at": iso(effective_at),
                "version": version,
                "members": self._current_members(conn, group_id),
            }
            self._record_audit(
                conn,
                group_id=group_id,
                event_type="membership_changed",
                entity_type="member",
                entity_id=payload.member_id,
                payload=response,
                group_version=version,
                occurred_at=effective_at,
            )
            self._store_idempotent_response(
                conn,
                self._idempotency_scope(f"group.membership.{group_id}", metadata),
                request_data,
                response,
            )
            return response

    def create_fx_rate(self, payload: FXRateCreate, metadata: RequestMetadata) -> dict[str, Any]:
        request_data = payload.model_dump(mode="json")
        with write_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(conn, self._idempotency_scope("fx.create", metadata), request_data)
            if replay is not None:
                return replay

            if payload.base_currency == payload.quote_currency:
                raise ValidationError("base_currency and quote_currency must differ")

            rate_id = str(uuid4())
            effective_at = coerce_time(payload.effective_at)
            response = {
                "id": rate_id,
                "base_currency": payload.base_currency,
                "quote_currency": payload.quote_currency,
                "rate": decimal_text(payload.rate),
                "effective_at": iso(effective_at),
                "source": payload.source,
            }
            conn.execute(
                """
                INSERT INTO fx_rates(id, base_currency, quote_currency, rate, effective_at, source, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rate_id,
                    payload.base_currency,
                    payload.quote_currency,
                    decimal_text(payload.rate),
                    iso(effective_at),
                    payload.source,
                    iso(now_utc()),
                ),
            )
            self._store_idempotent_response(conn, self._idempotency_scope("fx.create", metadata), request_data, response)
            return response

    def create_expense(self, payload: ExpenseCreate, metadata: RequestMetadata) -> dict[str, Any]:
        request_data = payload.model_dump(mode="json")
        with write_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(
                conn,
                self._idempotency_scope(f"expense.create.{payload.group_id}", metadata),
                request_data,
            )
            if replay is not None:
                return replay

            group = self._get_group_row(conn, payload.group_id)
            if payload.expected_version and group["version"] != payload.expected_version:
                raise ConflictError(f"Expected group version {payload.expected_version}, found {group['version']}")

            occurred_at = coerce_time(payload.occurred_at)
            member_state = self._members_as_of(conn, payload.group_id, occurred_at)
            active_member_ids = {member_id for member_id, state in member_state.items() if state["active"]}
            if payload.paid_by not in active_member_ids:
                raise ValidationError("paid_by must be active at the time of the expense")

            participant_ids = payload.participant_ids or [entry.member_id for entry in payload.allocations]
            if not participant_ids:
                raise ValidationError("At least one participant is required")
            for participant_id in participant_ids:
                if participant_id not in active_member_ids:
                    raise ValidationError(f"Participant {participant_id} is not active at the time of the expense")

            amount = quantize_money(payload.amount, payload.currency_code)
            allocations = compute_allocations(
                total=amount,
                currency_code=payload.currency_code,
                split_mode=payload.split_mode,
                participant_ids=participant_ids,
                allocations=[entry.model_dump() for entry in payload.allocations],
            )

            if payload.currency_code != group["base_currency"]:
                self._convert_amount(
                    conn,
                    amount=amount,
                    source_currency=payload.currency_code,
                    target_currency=group["base_currency"],
                    as_of=occurred_at,
                    pivot_currency=group["base_currency"],
                )

            expense_id = str(uuid4())
            version = self._bump_group_version(conn, payload.group_id)
            split_payload = {
                "participant_ids": sorted(dict.fromkeys(participant_ids)),
                "allocations": {member_id: decimal_text(value) for member_id, value in allocations.items()},
            }
            conn.execute(
                """
                INSERT INTO expenses(id, group_id, paid_by, amount, currency_code, occurred_at, description, split_mode, split_payload, recurring_template_id, version, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    expense_id,
                    payload.group_id,
                    payload.paid_by,
                    decimal_text(amount),
                    payload.currency_code,
                    iso(occurred_at),
                    payload.description,
                    payload.split_mode,
                    json_dumps(split_payload),
                    payload.recurring_template_id,
                    version,
                    iso(now_utc()),
                ),
            )

            for member_id, allocation in allocations.items():
                conn.execute(
                    """
                    INSERT INTO expense_allocations(expense_id, member_id, amount)
                    VALUES(?, ?, ?)
                    """,
                    (expense_id, member_id, decimal_text(allocation)),
                )
                if member_id == payload.paid_by:
                    continue
                conn.execute(
                    """
                    INSERT INTO ledger_entries(id, group_id, event_id, event_type, member_from, member_to, amount, currency_code, occurred_at, created_at)
                    VALUES(?, ?, ?, 'expense', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid4()),
                        payload.group_id,
                        expense_id,
                        member_id,
                        payload.paid_by,
                        decimal_text(allocation),
                        payload.currency_code,
                        iso(occurred_at),
                        iso(now_utc()),
                    ),
                )

            response = {
                "id": expense_id,
                "group_id": payload.group_id,
                "paid_by": payload.paid_by,
                "amount": decimal_text(amount),
                "currency_code": payload.currency_code,
                "occurred_at": iso(occurred_at),
                "description": payload.description,
                "split_mode": payload.split_mode,
                "allocations": {member_id: decimal_text(value) for member_id, value in allocations.items()},
                "version": version,
                "recurring_template_id": payload.recurring_template_id,
            }
            self._record_audit(
                conn,
                group_id=payload.group_id,
                event_type="expense_created",
                entity_type="expense",
                entity_id=expense_id,
                payload=response,
                group_version=version,
                occurred_at=occurred_at,
            )
            self._store_idempotent_response(
                conn,
                self._idempotency_scope(f"expense.create.{payload.group_id}", metadata),
                request_data,
                response,
            )
            return response

    def create_recurring_template(
        self,
        payload: RecurringExpenseCreate,
        metadata: RequestMetadata,
    ) -> dict[str, Any]:
        request_data = payload.model_dump(mode="json")
        with write_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(
                conn,
                self._idempotency_scope(f"recurring.create.{payload.group_id}", metadata),
                request_data,
            )
            if replay is not None:
                return replay

            group = self._get_group_row(conn, payload.group_id)
            if payload.expected_version and group["version"] != payload.expected_version:
                raise ConflictError(f"Expected group version {payload.expected_version}, found {group['version']}")

            start_at = coerce_time(payload.start_at)
            active_members = {member["member_id"] for member in self._current_members(conn, payload.group_id) if member["active"]}
            if payload.paid_by not in active_members:
                raise ValidationError("paid_by must be an active member")
            for participant_id in payload.participant_ids or [entry.member_id for entry in payload.allocations]:
                if participant_id not in active_members:
                    raise ValidationError(f"Participant {participant_id} is not an active member")

            template_id = str(uuid4())
            version = self._bump_group_version(conn, payload.group_id)
            split_payload = {
                "participant_ids": sorted(dict.fromkeys(payload.participant_ids)),
                "allocations": [entry.model_dump(mode="json") for entry in payload.allocations],
            }
            conn.execute(
                """
                INSERT INTO recurring_templates(id, group_id, paid_by, amount, currency_code, description, split_mode, split_payload, cadence_unit, cadence_count, start_at, next_run_at, ends_at, active, version, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    template_id,
                    payload.group_id,
                    payload.paid_by,
                    decimal_text(quantize_money(payload.amount, payload.currency_code)),
                    payload.currency_code,
                    payload.description,
                    payload.split_mode,
                    json_dumps(split_payload),
                    payload.cadence_unit,
                    payload.cadence_count,
                    iso(start_at),
                    iso(start_at),
                    iso(payload.ends_at) if payload.ends_at else None,
                    version,
                    iso(now_utc()),
                ),
            )
            response = {
                "id": template_id,
                "group_id": payload.group_id,
                "paid_by": payload.paid_by,
                "amount": decimal_text(quantize_money(payload.amount, payload.currency_code)),
                "currency_code": payload.currency_code,
                "description": payload.description,
                "split_mode": payload.split_mode,
                "participant_ids": payload.participant_ids,
                "allocations": [entry.model_dump(mode="json") for entry in payload.allocations],
                "cadence_unit": payload.cadence_unit,
                "cadence_count": payload.cadence_count,
                "start_at": iso(start_at),
                "ends_at": iso(payload.ends_at) if payload.ends_at else None,
                "next_run_at": iso(start_at),
                "version": version,
            }
            self._record_audit(
                conn,
                group_id=payload.group_id,
                event_type="recurring_template_created",
                entity_type="recurring_template",
                entity_id=template_id,
                payload=response,
                group_version=version,
                occurred_at=start_at,
            )
            self._store_idempotent_response(
                conn,
                self._idempotency_scope(f"recurring.create.{payload.group_id}", metadata),
                request_data,
                response,
            )
            return response

    def materialize_recurring(
        self,
        template_id: str,
        payload: MaterializeRecurringRequest,
        metadata: RequestMetadata,
    ) -> dict[str, Any]:
        request_data = {"template_id": template_id, **payload.model_dump(mode="json")}
        with read_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(
                conn,
                self._idempotency_scope(f"recurring.materialize.{template_id}", metadata),
                request_data,
            )
            if replay is not None:
                return replay

            row = conn.execute(
                """
                SELECT *
                FROM recurring_templates
                WHERE id = ? AND active = 1
                """,
                (template_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("Recurring template not found")

        through = coerce_time(payload.through)
        next_run_at = parse_time(row["next_run_at"])
        ends_at = parse_time(row["ends_at"]) if row["ends_at"] else None
        split_payload = json.loads(row["split_payload"])
        created: list[dict[str, Any]] = []

        while next_run_at <= through and (ends_at is None or next_run_at <= ends_at):
            expense_payload = ExpenseCreate(
                group_id=row["group_id"],
                paid_by=row["paid_by"],
                amount=Decimal(row["amount"]),
                currency_code=row["currency_code"],
                occurred_at=next_run_at,
                description=row["description"],
                participant_ids=split_payload.get("participant_ids", []),
                split_mode=row["split_mode"],
                allocations=[
                    {"member_id": item["member_id"], "value": item["value"]}
                    for item in split_payload.get("allocations", [])
                ],
                recurring_template_id=template_id,
            )
            created.append(
                self.create_expense(
                    expense_payload,
                    RequestMetadata(
                        source="recurring",
                        idempotency_key=f"{template_id}:{iso(next_run_at)}",
                    ),
                )
            )
            try:
                next_run_at = advance_time(next_run_at, row["cadence_unit"], row["cadence_count"])
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc

        response = {
            "template_id": template_id,
            "through": iso(through),
            "created_expenses": created,
            "next_run_at": iso(next_run_at),
        }
        with write_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE recurring_templates
                SET next_run_at = ?
                WHERE id = ?
                """,
                (iso(next_run_at), template_id),
            )
            self._store_idempotent_response(
                conn,
                self._idempotency_scope(f"recurring.materialize.{template_id}", metadata),
                request_data,
                response,
            )
        return response

    def record_settlement(self, payload: SettlementCreate, metadata: RequestMetadata) -> dict[str, Any]:
        request_data = payload.model_dump(mode="json")
        with write_connection(self.db_path) as conn:
            replay = self._load_idempotent_response(
                conn,
                self._idempotency_scope(f"settlement.create.{payload.group_id}", metadata),
                request_data,
            )
            if replay is not None:
                return replay

            group = self._get_group_row(conn, payload.group_id)
            if payload.expected_version and group["version"] != payload.expected_version:
                raise ConflictError(f"Expected group version {payload.expected_version}, found {group['version']}")

            occurred_at = coerce_time(payload.occurred_at)
            active_members = {member_id for member_id, state in self._members_as_of(conn, payload.group_id, occurred_at).items() if state["active"]}
            if payload.paid_by not in active_members or payload.received_by not in active_members:
                raise ValidationError("Both settlement participants must be active at the settlement time")

            if payload.currency_code != group["base_currency"]:
                self._convert_amount(
                    conn,
                    amount=quantize_money(payload.amount, payload.currency_code),
                    source_currency=payload.currency_code,
                    target_currency=group["base_currency"],
                    as_of=occurred_at,
                    pivot_currency=group["base_currency"],
                )

            settlement_id = str(uuid4())
            version = self._bump_group_version(conn, payload.group_id)
            amount = quantize_money(payload.amount, payload.currency_code)
            conn.execute(
                """
                INSERT INTO settlements(id, group_id, paid_by, received_by, amount, currency_code, occurred_at, description, version, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    settlement_id,
                    payload.group_id,
                    payload.paid_by,
                    payload.received_by,
                    decimal_text(amount),
                    payload.currency_code,
                    iso(occurred_at),
                    payload.description,
                    version,
                    iso(now_utc()),
                ),
            )
            conn.execute(
                """
                INSERT INTO ledger_entries(id, group_id, event_id, event_type, member_from, member_to, amount, currency_code, occurred_at, created_at)
                VALUES(?, ?, ?, 'settlement', ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    payload.group_id,
                    settlement_id,
                    payload.received_by,
                    payload.paid_by,
                    decimal_text(amount),
                    payload.currency_code,
                    iso(occurred_at),
                    iso(now_utc()),
                ),
            )

            response = {
                "id": settlement_id,
                "group_id": payload.group_id,
                "paid_by": payload.paid_by,
                "received_by": payload.received_by,
                "amount": decimal_text(amount),
                "currency_code": payload.currency_code,
                "occurred_at": iso(occurred_at),
                "description": payload.description,
                "version": version,
            }
            self._record_audit(
                conn,
                group_id=payload.group_id,
                event_type="settlement_recorded",
                entity_type="settlement",
                entity_id=settlement_id,
                payload=response,
                group_version=version,
                occurred_at=occurred_at,
            )
            self._store_idempotent_response(
                conn,
                self._idempotency_scope(f"settlement.create.{payload.group_id}", metadata),
                request_data,
                response,
            )
            return response

    def get_balances(
        self,
        group_id: str,
        *,
        settlement_currency: str | None,
        valuation_policy: str,
        as_of: datetime | None,
    ) -> dict[str, Any]:
        snapshot_time = coerce_time(as_of)
        with read_connection(self.db_path) as conn:
            group = self._get_group_row(conn, group_id)
            target_currency = (settlement_currency or group["base_currency"]).upper()
            entries = conn.execute(
                """
                SELECT *
                FROM ledger_entries
                WHERE group_id = ? AND occurred_at <= ?
                ORDER BY occurred_at, created_at, id
                """,
                (group_id, iso(snapshot_time)),
            ).fetchall()

            member_states = self._members_as_of(conn, group_id, snapshot_time)
            balances = {member_id: Decimal("0") for member_id in member_states}
            for entry in entries:
                rate_time = parse_time(entry["occurred_at"]) if valuation_policy == "expense_time" else snapshot_time
                converted = self._convert_amount(
                    conn,
                    amount=Decimal(entry["amount"]),
                    source_currency=entry["currency_code"],
                    target_currency=target_currency,
                    as_of=rate_time,
                    pivot_currency=group["base_currency"],
                )
                balances[entry["member_from"]] = balances.get(entry["member_from"], Decimal("0")) - converted
                balances[entry["member_to"]] = balances.get(entry["member_to"], Decimal("0")) + converted

            balance_items = [
                {
                    "member_id": member_id,
                    "display_name": state["display_name"],
                    "active": state["active"],
                    "net_amount": decimal_text(quantize_money(amount, target_currency)),
                }
                for member_id, amount in sorted(balances.items())
                for state in [member_states[member_id]]
            ]
            return {
                "group_id": group_id,
                "group_version": group["version"],
                "valuation_policy": valuation_policy,
                "settlement_currency": target_currency,
                "as_of": iso(snapshot_time),
                "balances": balance_items,
            }

    def get_settlement_plan(
        self,
        group_id: str,
        *,
        settlement_currency: str | None,
        valuation_policy: str,
        as_of: datetime | None,
    ) -> dict[str, Any]:
        balances = self.get_balances(
            group_id,
            settlement_currency=settlement_currency,
            valuation_policy=valuation_policy,
            as_of=as_of,
        )
        target_currency = balances["settlement_currency"]
        net_amounts = {
            item["member_id"]: Decimal(item["net_amount"])
            for item in balances["balances"]
            if Decimal(item["net_amount"]) != 0
        }
        transfer_plan = build_transfer_plan(net_amounts, target_currency)
        return {
            **balances,
            "transfer_plan_strategy": transfer_plan["strategy"],
            "transfers": [
                {**transfer, "currency_code": target_currency}
                for transfer in transfer_plan["transfers"]
            ],
        }

    def get_audit_history(self, group_id: str, limit: int) -> dict[str, Any]:
        with read_connection(self.db_path) as conn:
            self._get_group_row(conn, group_id)
            rows = conn.execute(
                """
                SELECT event_type, entity_type, entity_id, group_version, occurred_at, created_at, payload_json
                FROM audit_events
                WHERE group_id = ?
                ORDER BY occurred_at DESC, created_at DESC, id DESC
                LIMIT ?
                """,
                (group_id, limit),
            ).fetchall()
            return {
                "group_id": group_id,
                "events": [
                    {
                        "event_type": row["event_type"],
                        "entity_type": row["entity_type"],
                        "entity_id": row["entity_id"],
                        "group_version": row["group_version"],
                        "occurred_at": row["occurred_at"],
                        "recorded_at": row["created_at"],
                        "payload": json.loads(row["payload_json"]),
                    }
                    for row in rows
                ],
            }

    def sync(self, payload: SyncRequest) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for operation in payload.operations:
            with read_connection(self.db_path) as conn:
                existing = conn.execute(
                    """
                    SELECT status, result_json, conflict_json
                    FROM sync_operations
                    WHERE device_id = ? AND client_operation_id = ?
                    """,
                    (payload.device_id, operation.client_operation_id),
                ).fetchone()
            if existing is not None:
                results.append(
                    {
                        "client_operation_id": operation.client_operation_id,
                        "status": existing["status"],
                        "result": json.loads(existing["result_json"]) if existing["result_json"] else None,
                        "conflict": json.loads(existing["conflict_json"]) if existing["conflict_json"] else None,
                    }
                )
                continue

            try:
                result = self._apply_sync_operation(payload.device_id, operation)
                status = "applied"
                conflict = None
            except ConflictError as error:
                result = None
                status = "conflict"
                conflict = {"message": str(error)}

            with write_connection(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO sync_operations(id, device_id, client_operation_id, operation_type, group_id, expected_group_version, status, result_json, conflict_json, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid4()),
                        payload.device_id,
                        operation.client_operation_id,
                        operation.operation_type,
                        operation.group_id,
                        operation.expected_group_version,
                        status,
                        json_dumps(result) if result is not None else None,
                        json_dumps(conflict) if conflict is not None else None,
                        iso(now_utc()),
                    ),
                )
            results.append(
                {
                    "client_operation_id": operation.client_operation_id,
                    "status": status,
                    "result": result,
                    "conflict": conflict,
                }
            )
        return {"device_id": payload.device_id, "results": results}

    def _apply_sync_operation(self, device_id: str, operation: Any) -> dict[str, Any]:
        metadata = RequestMetadata(
            idempotency_key=f"{device_id}:{operation.client_operation_id}",
            source="sync",
            device_id=device_id,
            client_operation_id=operation.client_operation_id,
        )

        if operation.operation_type == "membership_change":
            if not operation.group_id:
                raise ValidationError("group_id is required for membership_change")
            payload = MembershipChange.model_validate(
                {**operation.payload, "expected_version": operation.expected_group_version}
            )
            return self.change_membership(operation.group_id, payload, metadata)
        if operation.operation_type == "add_expense":
            payload = ExpenseCreate.model_validate(
                {
                    **operation.payload,
                    "group_id": operation.group_id or operation.payload.get("group_id"),
                    "expected_version": operation.expected_group_version,
                }
            )
            return self.create_expense(payload, metadata)
        if operation.operation_type == "record_settlement":
            payload = SettlementCreate.model_validate(
                {
                    **operation.payload,
                    "group_id": operation.group_id or operation.payload.get("group_id"),
                    "expected_version": operation.expected_group_version,
                }
            )
            return self.record_settlement(payload, metadata)
        if operation.operation_type == "create_recurring_template":
            payload = RecurringExpenseCreate.model_validate(
                {
                    **operation.payload,
                    "group_id": operation.group_id or operation.payload.get("group_id"),
                    "expected_version": operation.expected_group_version,
                }
            )
            return self.create_recurring_template(payload, metadata)
        if operation.operation_type == "materialize_recurring":
            template_id = operation.payload.get("template_id")
            if not template_id:
                raise ValidationError("template_id is required for materialize_recurring")
            payload = MaterializeRecurringRequest.model_validate(operation.payload)
            return self.materialize_recurring(template_id, payload, metadata)
        raise ValidationError(f"Unsupported operation_type: {operation.operation_type}")

    def _current_members(self, conn: Any, group_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT member_id, display_name, active, joined_at, left_at
            FROM group_members
            WHERE group_id = ?
            ORDER BY member_id
            """,
            (group_id,),
        ).fetchall()
        return [
            {
                "member_id": row["member_id"],
                "display_name": row["display_name"],
                "active": bool(row["active"]),
                "joined_at": row["joined_at"],
                "left_at": row["left_at"],
            }
            for row in rows
        ]

    def _members_as_of(self, conn: Any, group_id: str, snapshot_time: datetime) -> dict[str, dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT member_id, action, display_name, effective_at
            FROM membership_events
            WHERE group_id = ? AND effective_at <= ?
            ORDER BY effective_at, created_at, id
            """,
            (group_id, iso(snapshot_time)),
        ).fetchall()

        state: dict[str, dict[str, Any]] = {}
        for row in rows:
            member_id = row["member_id"]
            if row["action"] == "add":
                existing = state.get(member_id, {})
                state[member_id] = {
                    "member_id": member_id,
                    "display_name": row["display_name"] or existing.get("display_name") or member_id,
                    "active": True,
                    "joined_at": row["effective_at"],
                    "left_at": None,
                }
            else:
                if member_id in state:
                    state[member_id]["active"] = False
                    state[member_id]["left_at"] = row["effective_at"]
        return state

    def _convert_amount(
        self,
        conn: Any,
        *,
        amount: Decimal,
        source_currency: str,
        target_currency: str,
        as_of: datetime,
        pivot_currency: str | None,
    ) -> Decimal:
        if source_currency == target_currency:
            return quantize_money(amount, target_currency)

        direct = conn.execute(
            """
            SELECT rate
            FROM fx_rates
            WHERE base_currency = ? AND quote_currency = ? AND effective_at <= ?
            ORDER BY effective_at DESC, created_at DESC, id DESC
            LIMIT 1
            """,
            (source_currency, target_currency, iso(as_of)),
        ).fetchone()
        if direct is not None:
            return quantize_money(amount * Decimal(direct["rate"]), target_currency)

        inverse = conn.execute(
            """
            SELECT rate
            FROM fx_rates
            WHERE base_currency = ? AND quote_currency = ? AND effective_at <= ?
            ORDER BY effective_at DESC, created_at DESC, id DESC
            LIMIT 1
            """,
            (target_currency, source_currency, iso(as_of)),
        ).fetchone()
        if inverse is not None:
            return quantize_money(amount / Decimal(inverse["rate"]), target_currency)

        if pivot_currency and source_currency != pivot_currency and target_currency != pivot_currency:
            via_pivot = self._convert_amount(
                conn,
                amount=amount,
                source_currency=source_currency,
                target_currency=pivot_currency,
                as_of=as_of,
                pivot_currency=None,
            )
            return self._convert_amount(
                conn,
                amount=via_pivot,
                source_currency=pivot_currency,
                target_currency=target_currency,
                as_of=as_of,
                pivot_currency=None,
            )

        raise ValidationError(
            f"Missing FX rate for {source_currency}->{target_currency} at {iso(as_of)}"
        )

    def _get_group_row(self, conn: Any, group_id: str) -> Any:
        row = conn.execute("SELECT * FROM groups WHERE id = ?", (group_id,)).fetchone()
        if row is None:
            raise NotFoundError("Group not found")
        return row

    def _bump_group_version(self, conn: Any, group_id: str) -> int:
        conn.execute("UPDATE groups SET version = version + 1 WHERE id = ?", (group_id,))
        row = self._get_group_row(conn, group_id)
        return int(row["version"])

    def _record_audit(
        self,
        conn: Any,
        *,
        group_id: str,
        event_type: str,
        entity_type: str,
        entity_id: str,
        payload: dict[str, Any],
        group_version: int,
        occurred_at: datetime,
    ) -> None:
        conn.execute(
            """
            INSERT INTO audit_events(id, group_id, event_type, entity_type, entity_id, payload_json, group_version, occurred_at, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                group_id,
                event_type,
                entity_type,
                entity_id,
                json_dumps(payload),
                group_version,
                iso(occurred_at),
                iso(now_utc()),
            ),
        )

    def _idempotency_scope(self, prefix: str, metadata: RequestMetadata) -> str | None:
        if not metadata.idempotency_key:
            return None
        return f"{prefix}:{metadata.idempotency_key}"

    def _load_idempotent_response(
        self,
        conn: Any,
        scope: str | None,
        request_data: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not scope:
            return None
        row = conn.execute(
            "SELECT request_hash, response_json FROM idempotency_records WHERE scope = ?",
            (scope,),
        ).fetchone()
        if row is None:
            return None
        if row["request_hash"] != request_hash(request_data):
            raise ConflictError("Idempotency key has already been used with a different payload")
        return json.loads(row["response_json"])

    def _store_idempotent_response(
        self,
        conn: Any,
        scope: str | None,
        request_data: dict[str, Any],
        response: dict[str, Any],
    ) -> None:
        if not scope:
            return
        conn.execute(
            """
            INSERT OR REPLACE INTO idempotency_records(scope, request_hash, response_json, created_at)
            VALUES(?, ?, ?, ?)
            """,
            (scope, request_hash(request_data), json_dumps(response), iso(now_utc())),
        )
