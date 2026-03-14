"""Export service for the Expense Splitter API.

Handles CSV and JSON export generation with filtering, date ranges,
summary rows, and export job tracking.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from internal.models.export import (
    DEFAULT_EXPORT_COLUMNS,
    VALID_EXPORT_COLUMNS,
    ExportRequest,
)
from internal.service.ledger_service import NotFoundError, ValidationError
from internal.storage.sqlite import read_connection, write_connection
from internal.utils.date_helpers import ensure_utc
from internal.utils.formatters import format_csv_row, format_money


class ExportService:
    """Generates data exports for groups in CSV or JSON format."""

    def __init__(self, db_path=None):
        self._db_path = db_path

    def create_export(
        self,
        payload: ExportRequest,
    ) -> dict[str, Any]:
        """Create an export job and generate the export data synchronously.

        Returns the export job record with the generated data.
        """
        self._validate_request(payload)

        with write_connection(self._db_path) as conn:
            group = conn.execute(
                "SELECT id, name, base_currency FROM groups WHERE id = ?",
                (payload.group_id,),
            ).fetchone()
            if not group:
                raise NotFoundError(f"Group {payload.group_id} not found")

            export_id = str(uuid4())
            now = _iso_now()

            conn.execute(
                """
                INSERT INTO export_jobs
                    (id, group_id, format, status, filters_json,
                     columns_json, created_at, completed_at, result_json)
                VALUES (?, ?, ?, 'processing', ?, ?, ?, NULL, NULL)
                """,
                (
                    export_id,
                    payload.group_id,
                    payload.format,
                    json.dumps(payload.filters.model_dump()),
                    json.dumps(payload.columns or DEFAULT_EXPORT_COLUMNS),
                    now,
                ),
            )

        try:
            data = self._generate_export_data(payload)
            result = self._format_output(data, payload)

            with write_connection(self._db_path) as conn:
                conn.execute(
                    """
                    UPDATE export_jobs
                    SET status = 'completed', completed_at = ?, result_json = ?
                    WHERE id = ?
                    """,
                    (_iso_now(), json.dumps(result, default=str), export_id),
                )

            return {
                "export_id": export_id,
                "group_id": payload.group_id,
                "format": payload.format,
                "status": "completed",
                "row_count": result.get("row_count", 0),
                "created_at": now,
                "completed_at": _iso_now(),
                "data": result,
            }

        except Exception as exc:
            with write_connection(self._db_path) as conn:
                conn.execute(
                    "UPDATE export_jobs SET status = 'failed', completed_at = ? WHERE id = ?",
                    (_iso_now(), export_id),
                )
            raise

    def get_export(self, group_id: str, export_id: str) -> dict[str, Any]:
        """Retrieve an export job by ID."""
        with read_connection(self._db_path) as conn:
            row = conn.execute(
                "SELECT * FROM export_jobs WHERE id = ? AND group_id = ?",
                (export_id, group_id),
            ).fetchone()
            if not row:
                raise NotFoundError(f"Export job {export_id} not found")

            result = {
                "export_id": row["id"],
                "group_id": row["group_id"],
                "format": row["format"],
                "status": row["status"],
                "created_at": row["created_at"],
                "completed_at": row["completed_at"],
            }
            if row["result_json"]:
                result["data"] = json.loads(row["result_json"])
            return result

    def list_exports(
        self,
        group_id: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List export jobs for a group."""
        with read_connection(self._db_path) as conn:
            count_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM export_jobs WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            total = count_row["cnt"] if count_row else 0

            rows = conn.execute(
                """
                SELECT id, group_id, format, status, created_at, completed_at
                FROM export_jobs WHERE group_id = ?
                ORDER BY created_at DESC LIMIT ? OFFSET ?
                """,
                (group_id, limit, offset),
            ).fetchall()

            exports = [
                {
                    "export_id": row["id"],
                    "group_id": row["group_id"],
                    "format": row["format"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "completed_at": row["completed_at"],
                }
                for row in rows
            ]

            return {"exports": exports, "total": total, "limit": limit, "offset": offset}

    # ── Data Generation ──────────────────────────────────────────────

    def _generate_export_data(self, payload: ExportRequest) -> list[dict[str, Any]]:
        """Query and assemble raw export data from the database."""
        records: list[dict[str, Any]] = []

        with read_connection(self._db_path) as conn:
            group = conn.execute(
                "SELECT name, base_currency FROM groups WHERE id = ?",
                (payload.group_id,),
            ).fetchone()
            group_name = group["name"] if group else ""
            base_currency = group["base_currency"] if group else "USD"

            # Fetch expenses
            expense_query = """
                SELECT e.id, e.paid_by, e.amount, e.currency_code,
                       e.occurred_at, e.description, e.split_mode,
                       e.recurring_template_id
                FROM expenses e
                WHERE e.group_id = ?
            """
            params: list[Any] = [payload.group_id]

            if payload.start_date:
                expense_query += " AND e.occurred_at >= ?"
                params.append(ensure_utc(payload.start_date).isoformat())
            if payload.end_date:
                expense_query += " AND e.occurred_at <= ?"
                params.append(ensure_utc(payload.end_date).isoformat())

            expense_query += " ORDER BY e.occurred_at ASC"
            expense_rows = conn.execute(expense_query, params).fetchall()

            for row in expense_rows:
                if not self._passes_filters(row, payload, conn):
                    continue

                alloc_rows = conn.execute(
                    "SELECT member_id FROM expense_allocations WHERE expense_id = ?",
                    (row["id"],),
                ).fetchall()
                participants = [a["member_id"] for a in alloc_rows]

                records.append({
                    "type": "expense",
                    "expense_id": row["id"],
                    "date": row["occurred_at"],
                    "description": row["description"] or "",
                    "paid_by": row["paid_by"],
                    "amount": row["amount"],
                    "currency": row["currency_code"],
                    "split_mode": row["split_mode"],
                    "participants": ", ".join(sorted(participants)),
                    "category": self._categorize_description(row["description"] or ""),
                    "recurring_template_id": row["recurring_template_id"] or "",
                    "group_name": group_name,
                })

            # Fetch settlements
            if payload.filters.include_settlements:
                settle_query = """
                    SELECT s.id, s.paid_by, s.received_by, s.amount,
                           s.currency_code, s.occurred_at, s.description
                    FROM settlements s
                    WHERE s.group_id = ?
                """
                settle_params: list[Any] = [payload.group_id]

                if payload.start_date:
                    settle_query += " AND s.occurred_at >= ?"
                    settle_params.append(ensure_utc(payload.start_date).isoformat())
                if payload.end_date:
                    settle_query += " AND s.occurred_at <= ?"
                    settle_params.append(ensure_utc(payload.end_date).isoformat())

                settle_query += " ORDER BY s.occurred_at ASC"
                settle_rows = conn.execute(settle_query, settle_params).fetchall()

                for row in settle_rows:
                    records.append({
                        "type": "settlement",
                        "expense_id": row["id"],
                        "date": row["occurred_at"],
                        "description": row["description"] or "Settlement",
                        "paid_by": row["paid_by"],
                        "amount": row["amount"],
                        "currency": row["currency_code"],
                        "split_mode": "",
                        "participants": row["received_by"],
                        "received_by": row["received_by"],
                        "settlement_amount": row["amount"],
                        "category": "settlement",
                        "recurring_template_id": "",
                        "group_name": group_name,
                    })

        return records

    def _passes_filters(self, row, payload: ExportRequest, conn) -> bool:
        """Check if an expense row passes the configured filters."""
        filters = payload.filters

        if filters.member_ids:
            if row["paid_by"] not in filters.member_ids:
                allocs = conn.execute(
                    "SELECT member_id FROM expense_allocations WHERE expense_id = ?",
                    (row["id"],),
                ).fetchall()
                participant_ids = {a["member_id"] for a in allocs}
                if not participant_ids.intersection(filters.member_ids):
                    return False

        if filters.currency_codes:
            if row["currency_code"] not in [c.upper() for c in filters.currency_codes]:
                return False

        if filters.split_modes:
            if row["split_mode"] not in filters.split_modes:
                return False

        if filters.min_amount:
            if Decimal(row["amount"]) < Decimal(filters.min_amount):
                return False

        if filters.max_amount:
            if Decimal(row["amount"]) > Decimal(filters.max_amount):
                return False

        if not filters.include_recurring and row["recurring_template_id"]:
            return False

        if filters.categories:
            category = self._categorize_description(row["description"] or "")
            if category not in filters.categories:
                return False

        return True

    def _categorize_description(self, description: str) -> str:
        """Auto-categorize an expense based on its description keywords."""
        desc_lower = description.lower()
        from internal.models.budget import DEFAULT_CATEGORY_RULES

        for rule in sorted(
            DEFAULT_CATEGORY_RULES, key=lambda r: r["priority"], reverse=True
        ):
            keywords = rule["keywords"]
            if not keywords:
                continue
            for keyword in keywords:
                if keyword in desc_lower:
                    return rule["category"]
        return "other"

    # ── Output Formatting ────────────────────────────────────────────

    def _format_output(
        self,
        records: list[dict[str, Any]],
        payload: ExportRequest,
    ) -> dict[str, Any]:
        """Format records as CSV or JSON output."""
        columns = payload.columns or DEFAULT_EXPORT_COLUMNS

        if payload.format == "csv":
            return self._format_csv(records, columns, payload)
        return self._format_json(records, columns, payload)

    def _format_csv(
        self,
        records: list[dict[str, Any]],
        columns: list[str],
        payload: ExportRequest,
    ) -> dict[str, Any]:
        """Generate CSV string output."""
        lines = [format_csv_row(columns)]

        for record in records:
            row_values = [str(record.get(col, "")) for col in columns]
            lines.append(format_csv_row(row_values))

        if payload.include_summary and records:
            lines.append("")
            lines.append(format_csv_row(["--- Summary ---"]))
            lines.append(format_csv_row(["Total Records", str(len(records))]))

            total_by_currency: dict[str, Decimal] = {}
            for record in records:
                currency = record.get("currency", "USD")
                amount = Decimal(str(record.get("amount", "0")))
                total_by_currency[currency] = total_by_currency.get(currency, Decimal("0")) + amount

            for currency, total in sorted(total_by_currency.items()):
                lines.append(format_csv_row([f"Total ({currency})", str(total)]))

        return {
            "content": "\n".join(lines),
            "row_count": len(records),
            "format": "csv",
        }

    def _format_json(
        self,
        records: list[dict[str, Any]],
        columns: list[str],
        payload: ExportRequest,
    ) -> dict[str, Any]:
        """Generate JSON structured output."""
        filtered_records = []
        for record in records:
            filtered_records.append({col: record.get(col) for col in columns})

        result: dict[str, Any] = {
            "records": filtered_records,
            "row_count": len(records),
            "format": "json",
        }

        if payload.include_summary and records:
            total_by_currency: dict[str, str] = {}
            for record in records:
                currency = record.get("currency", "USD")
                amount = Decimal(str(record.get("amount", "0")))
                prev = Decimal(total_by_currency.get(currency, "0"))
                total_by_currency[currency] = str(prev + amount)

            result["summary"] = {
                "total_records": len(records),
                "totals_by_currency": total_by_currency,
            }

        return result

    def _validate_request(self, payload: ExportRequest) -> None:
        """Validate export request parameters."""
        if payload.columns:
            invalid = set(payload.columns) - VALID_EXPORT_COLUMNS
            if invalid:
                raise ValidationError(
                    f"Invalid export columns: {', '.join(sorted(invalid))}"
                )


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()
