"""Budget service for the SettleUp API.

Manages budget creation, spending tracking, threshold alerts,
auto-categorization of expenses, and period-based rollover logic.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from uuid import uuid4

from internal.models.budget import (
    BudgetCreate,
    BudgetUpdate,
    CategoryRule,
    CategoryRuleSet,
    DEFAULT_CATEGORY_RULES,
)
from internal.service.ledger_service import NotFoundError, ServiceError, ValidationError
from internal.service.split_service import quantize_money
from internal.storage.sqlite import read_connection, write_connection
from internal.utils.date_helpers import ensure_utc, now_utc, period_start, period_end


class BudgetService:
    """Handles group budgets, spending tracking, and threshold alerts."""

    def __init__(self, db_path=None):
        self._db_path = db_path

    # ── Budget CRUD ─────────────────────────────────────────────────

    def create_budget(self, payload: BudgetCreate) -> dict[str, Any]:
        """Create a new budget for a group and category."""
        with write_connection(self._db_path) as conn:
            group = conn.execute(
                "SELECT id, base_currency FROM groups WHERE id = ?",
                (payload.group_id,),
            ).fetchone()
            if not group:
                raise NotFoundError(f"Group {payload.group_id} not found")

            existing = conn.execute(
                """
                SELECT id FROM budgets
                WHERE group_id = ? AND category = ? AND active = 1
                """,
                (payload.group_id, payload.category),
            ).fetchone()
            if existing:
                raise ValidationError(
                    f"Active budget already exists for category '{payload.category}' "
                    f"in group {payload.group_id}"
                )

            budget_id = str(uuid4())
            now = _iso_now()

            conn.execute(
                """
                INSERT INTO budgets
                    (id, group_id, category, amount, currency_code, period,
                     alert_thresholds_json, rollover, notes, active,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    budget_id,
                    payload.group_id,
                    payload.category,
                    str(payload.amount),
                    payload.currency_code,
                    payload.period,
                    json.dumps(payload.alert_thresholds),
                    int(payload.rollover),
                    payload.notes,
                    now,
                    now,
                ),
            )

            return {
                "budget_id": budget_id,
                "group_id": payload.group_id,
                "category": payload.category,
                "amount": str(payload.amount),
                "currency_code": payload.currency_code,
                "period": payload.period,
                "alert_thresholds": payload.alert_thresholds,
                "rollover": payload.rollover,
                "notes": payload.notes,
                "active": True,
                "created_at": now,
            }

    def get_budget(
        self, group_id: str, budget_id: str,
    ) -> dict[str, Any]:
        """Get a budget with current spending information."""
        with read_connection(self._db_path) as conn:
            row = conn.execute(
                "SELECT * FROM budgets WHERE id = ? AND group_id = ?",
                (budget_id, group_id),
            ).fetchone()
            if not row:
                raise NotFoundError(f"Budget {budget_id} not found")

            budget = _row_to_budget_dict(row)
            spending = self._compute_spending(conn, row)
            budget.update(spending)
            return budget

    def list_budgets(
        self, group_id: str, *, include_inactive: bool = False,
    ) -> dict[str, Any]:
        """List all budgets for a group with current spending."""
        with read_connection(self._db_path) as conn:
            query = "SELECT * FROM budgets WHERE group_id = ?"
            params: list[Any] = [group_id]
            if not include_inactive:
                query += " AND active = 1"
            query += " ORDER BY category ASC"

            rows = conn.execute(query, params).fetchall()
            budgets = []
            for row in rows:
                budget = _row_to_budget_dict(row)
                budget.update(self._compute_spending(conn, row))
                budgets.append(budget)

            return {"budgets": budgets, "total": len(budgets)}

    def update_budget(
        self, group_id: str, budget_id: str, payload: BudgetUpdate,
    ) -> dict[str, Any]:
        """Update an existing budget's parameters."""
        with write_connection(self._db_path) as conn:
            existing = conn.execute(
                "SELECT * FROM budgets WHERE id = ? AND group_id = ? AND active = 1",
                (budget_id, group_id),
            ).fetchone()
            if not existing:
                raise NotFoundError(f"Active budget {budget_id} not found")

            updates: dict[str, Any] = {}
            if payload.amount is not None:
                updates["amount"] = str(payload.amount)
            if payload.period is not None:
                updates["period"] = payload.period
            if payload.alert_thresholds is not None:
                updates["alert_thresholds_json"] = json.dumps(payload.alert_thresholds)
            if payload.rollover is not None:
                updates["rollover"] = int(payload.rollover)
            if payload.notes is not None:
                updates["notes"] = payload.notes

            if not updates:
                return self.get_budget(group_id, budget_id)

            updates["updated_at"] = _iso_now()
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [budget_id, group_id]
            conn.execute(
                f"UPDATE budgets SET {set_clause} WHERE id = ? AND group_id = ?",
                values,
            )

        return self.get_budget(group_id, budget_id)

    def delete_budget(self, group_id: str, budget_id: str) -> dict[str, Any]:
        """Soft-delete a budget by marking it inactive."""
        with write_connection(self._db_path) as conn:
            existing = conn.execute(
                "SELECT id FROM budgets WHERE id = ? AND group_id = ? AND active = 1",
                (budget_id, group_id),
            ).fetchone()
            if not existing:
                raise NotFoundError(f"Active budget {budget_id} not found")

            conn.execute(
                "UPDATE budgets SET active = 0, updated_at = ? WHERE id = ?",
                (_iso_now(), budget_id),
            )
            return {"deleted": True, "budget_id": budget_id}

    # ── Budget Summary ──────────────────────────────────────────────

    def get_budget_summary(self, group_id: str) -> dict[str, Any]:
        """Get an aggregated budget summary across all active budgets."""
        with read_connection(self._db_path) as conn:
            group = conn.execute(
                "SELECT id, base_currency FROM groups WHERE id = ?",
                (group_id,),
            ).fetchone()
            if not group:
                raise NotFoundError(f"Group {group_id} not found")

            rows = conn.execute(
                "SELECT * FROM budgets WHERE group_id = ? AND active = 1 ORDER BY category",
                (group_id,),
            ).fetchall()

            total_budget = Decimal("0")
            total_spent = Decimal("0")
            categories: list[dict[str, Any]] = []
            alerts: list[dict[str, Any]] = []

            for row in rows:
                spending = self._compute_spending(conn, row)
                budget_amount = Decimal(row["amount"])
                spent_amount = Decimal(str(spending["current_spend"]))
                total_budget += budget_amount
                total_spent += spent_amount

                entry = {
                    "category": row["category"],
                    "budget_amount": str(budget_amount),
                    "current_spend": str(spent_amount),
                    "remaining": str(budget_amount - spent_amount),
                    "utilization_pct": spending["utilization_pct"],
                    "status": spending["status"],
                }
                categories.append(entry)

                if spending["triggered_alerts"]:
                    for alert in spending["triggered_alerts"]:
                        alerts.append({
                            "category": row["category"],
                            "threshold_pct": alert,
                            "current_pct": spending["utilization_pct"],
                        })

            overall_pct = (
                float(total_spent / total_budget * 100)
                if total_budget > 0
                else 0.0
            )

            return {
                "group_id": group_id,
                "total_budget": str(total_budget),
                "total_spent": str(total_spent),
                "total_remaining": str(total_budget - total_spent),
                "overall_utilization_pct": round(overall_pct, 1),
                "categories": categories,
                "active_alerts": alerts,
                "currency_code": group["base_currency"],
            }

    # ── Category Rules ──────────────────────────────────────────────

    def set_category_rules(
        self, payload: CategoryRuleSet,
    ) -> dict[str, Any]:
        """Set category rules for a group (replaces existing rules)."""
        with write_connection(self._db_path) as conn:
            group = conn.execute(
                "SELECT id FROM groups WHERE id = ?", (payload.group_id,)
            ).fetchone()
            if not group:
                raise NotFoundError(f"Group {payload.group_id} not found")

            conn.execute(
                "DELETE FROM category_rules WHERE group_id = ?",
                (payload.group_id,),
            )

            now = _iso_now()
            for rule in payload.rules:
                rule_id = str(uuid4())
                conn.execute(
                    """
                    INSERT INTO category_rules
                        (id, group_id, category, keywords_json, priority, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rule_id,
                        payload.group_id,
                        rule.category,
                        json.dumps(rule.keywords),
                        rule.priority,
                        now,
                    ),
                )

            return {
                "group_id": payload.group_id,
                "rules_count": len(payload.rules),
                "rules": [
                    {
                        "category": r.category,
                        "keywords": r.keywords,
                        "priority": r.priority,
                    }
                    for r in payload.rules
                ],
            }

    def get_category_rules(self, group_id: str) -> dict[str, Any]:
        """Get the category rules for a group."""
        with read_connection(self._db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM category_rules WHERE group_id = ? ORDER BY priority DESC, category ASC",
                (group_id,),
            ).fetchall()

            if not rows:
                return {
                    "group_id": group_id,
                    "rules": DEFAULT_CATEGORY_RULES,
                    "source": "default",
                }

            rules = [
                {
                    "category": row["category"],
                    "keywords": json.loads(row["keywords_json"]),
                    "priority": row["priority"],
                }
                for row in rows
            ]
            return {"group_id": group_id, "rules": rules, "source": "custom"}

    def categorize_expense(
        self,
        group_id: str,
        description: str,
    ) -> str:
        """Categorize an expense description using group-specific or default rules."""
        rules_data = self.get_category_rules(group_id)
        rules = rules_data["rules"]

        desc_lower = description.lower()
        for rule in sorted(rules, key=lambda r: r["priority"], reverse=True):
            for keyword in rule["keywords"]:
                if keyword in desc_lower:
                    return rule["category"]
        return "other"

    # ── Internal helpers ─────────────────────────────────────────────

    def _compute_spending(self, conn, budget_row) -> dict[str, Any]:
        """Compute current spending against a budget for the current period."""
        now = now_utc()
        period = budget_row["period"]
        currency = budget_row["currency_code"]
        budget_amount = Decimal(budget_row["amount"])
        category = budget_row["category"]

        period_unit = _period_to_unit(period)
        p_start = period_start(now, period_unit)
        p_end = period_end(now, period_unit)

        expenses = conn.execute(
            """
            SELECT e.amount, e.description, e.currency_code
            FROM expenses e
            WHERE e.group_id = ? AND e.occurred_at >= ? AND e.occurred_at <= ?
            """,
            (
                budget_row["group_id"],
                datetime(p_start.year, p_start.month, p_start.day, tzinfo=timezone.utc).isoformat(),
                datetime(p_end.year, p_end.month, p_end.day, 23, 59, 59, tzinfo=timezone.utc).isoformat(),
            ),
        ).fetchall()

        total_spend = Decimal("0")
        for expense in expenses:
            expense_category = self._categorize_from_rules(
                conn, budget_row["group_id"], expense["description"] or ""
            )
            if expense_category == category and expense["currency_code"] == currency:
                total_spend += Decimal(expense["amount"])

        utilization = (
            float(total_spend / budget_amount * 100)
            if budget_amount > 0
            else 0.0
        )
        utilization_rounded = round(utilization, 1)

        thresholds = json.loads(budget_row["alert_thresholds_json"])
        triggered = [t for t in thresholds if utilization >= t]

        remaining = budget_amount - total_spend
        if remaining < 0:
            status = "over_budget"
        elif utilization >= 80:
            status = "warning"
        elif utilization >= 50:
            status = "on_track"
        else:
            status = "healthy"

        return {
            "current_spend": str(quantize_money(total_spend, currency)),
            "remaining": str(quantize_money(remaining, currency)),
            "utilization_pct": utilization_rounded,
            "status": status,
            "triggered_alerts": triggered,
            "period_start": p_start.isoformat(),
            "period_end": p_end.isoformat(),
        }

    def _categorize_from_rules(self, conn, group_id: str, description: str) -> str:
        """Categorize using stored rules, falling back to defaults."""
        rows = conn.execute(
            "SELECT category, keywords_json, priority FROM category_rules WHERE group_id = ? ORDER BY priority DESC",
            (group_id,),
        ).fetchall()

        rules = [
            {"category": r["category"], "keywords": json.loads(r["keywords_json"]), "priority": r["priority"]}
            for r in rows
        ] if rows else DEFAULT_CATEGORY_RULES

        desc_lower = description.lower()
        for rule in rules:
            for keyword in rule["keywords"]:
                if keyword in desc_lower:
                    return rule["category"]
        return "other"


def _row_to_budget_dict(row) -> dict[str, Any]:
    """Convert a database row to a budget dict."""
    return {
        "budget_id": row["id"],
        "group_id": row["group_id"],
        "category": row["category"],
        "amount": row["amount"],
        "currency_code": row["currency_code"],
        "period": row["period"],
        "alert_thresholds": json.loads(row["alert_thresholds_json"]),
        "rollover": bool(row["rollover"]),
        "notes": row["notes"],
        "active": bool(row["active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _period_to_unit(period: str) -> str:
    """Convert a budget period to a date_helpers period unit."""
    mapping = {
        "weekly": "week",
        "monthly": "month",
        "quarterly": "quarter",
        "yearly": "year",
    }
    return mapping.get(period, "month")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()
