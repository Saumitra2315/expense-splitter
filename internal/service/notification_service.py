"""Notification service for the Expense Splitter API.

Manages notification preferences, event dispatch, digest computation,
and threshold-based alerts — all persisted in SQLite.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from internal.models.notification import (
    NotificationDigestRequest,
    NotificationEventCreate,
    NotificationPreferenceCreate,
    NotificationPreferenceUpdate,
)
from internal.service.ledger_service import NotFoundError, ServiceError, ValidationError
from internal.storage.sqlite import read_connection, write_connection
from internal.utils.date_helpers import ensure_utc, now_utc


class NotificationService:
    """Handles notification preferences, event dispatch, and digests."""

    def __init__(self, db_path=None):
        self._db_path = db_path

    # ── Preference CRUD ──────────────────────────────────────────────

    def set_preferences(
        self,
        group_id: str,
        payload: NotificationPreferenceCreate,
    ) -> dict[str, Any]:
        """Create or replace notification preferences for a member in a group."""
        with write_connection(self._db_path) as conn:
            group = conn.execute(
                "SELECT id FROM groups WHERE id = ?", (group_id,)
            ).fetchone()
            if not group:
                raise NotFoundError(f"Group {group_id} not found")

            member = conn.execute(
                "SELECT member_id FROM group_members WHERE group_id = ? AND member_id = ? AND active = 1",
                (group_id, payload.member_id),
            ).fetchone()
            if not member:
                raise NotFoundError(
                    f"Active member {payload.member_id} not found in group {group_id}"
                )

            pref_id = f"{group_id}:{payload.member_id}"
            now = _iso_now()

            conn.execute(
                """
                INSERT OR REPLACE INTO notification_preferences
                    (id, group_id, member_id, channel, enabled, event_types_json,
                     threshold_amount, quiet_hours_start, quiet_hours_end,
                     digest_frequency, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pref_id,
                    group_id,
                    payload.member_id,
                    payload.channel,
                    int(payload.enabled),
                    json.dumps(list(payload.event_types)),
                    str(payload.threshold_amount) if payload.threshold_amount is not None else None,
                    payload.quiet_hours_start,
                    payload.quiet_hours_end,
                    payload.digest_frequency,
                    now,
                    now,
                ),
            )

            return self._preference_to_dict(
                pref_id, group_id, payload.member_id, payload, now
            )

    def get_preferences(
        self,
        group_id: str,
        member_id: str,
    ) -> dict[str, Any]:
        """Retrieve notification preferences for a member."""
        with read_connection(self._db_path) as conn:
            row = conn.execute(
                "SELECT * FROM notification_preferences WHERE group_id = ? AND member_id = ?",
                (group_id, member_id),
            ).fetchone()
            if not row:
                raise NotFoundError(
                    f"No notification preferences for member {member_id} in group {group_id}"
                )
            return _row_to_preference_dict(row)

    def update_preferences(
        self,
        group_id: str,
        member_id: str,
        payload: NotificationPreferenceUpdate,
    ) -> dict[str, Any]:
        """Partially update notification preferences."""
        with write_connection(self._db_path) as conn:
            existing = conn.execute(
                "SELECT * FROM notification_preferences WHERE group_id = ? AND member_id = ?",
                (group_id, member_id),
            ).fetchone()
            if not existing:
                raise NotFoundError(
                    f"No notification preferences to update for member {member_id}"
                )

            updates: dict[str, Any] = {}
            if payload.channel is not None:
                updates["channel"] = payload.channel
            if payload.enabled is not None:
                updates["enabled"] = int(payload.enabled)
            if payload.event_types is not None:
                updates["event_types_json"] = json.dumps(list(payload.event_types))
            if payload.threshold_amount is not None:
                updates["threshold_amount"] = str(payload.threshold_amount)
            if payload.quiet_hours_start is not None:
                updates["quiet_hours_start"] = payload.quiet_hours_start
            if payload.quiet_hours_end is not None:
                updates["quiet_hours_end"] = payload.quiet_hours_end
            if payload.digest_frequency is not None:
                updates["digest_frequency"] = payload.digest_frequency

            if not updates:
                return _row_to_preference_dict(existing)

            updates["updated_at"] = _iso_now()
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [group_id, member_id]
            conn.execute(
                f"UPDATE notification_preferences SET {set_clause} WHERE group_id = ? AND member_id = ?",
                values,
            )

            row = conn.execute(
                "SELECT * FROM notification_preferences WHERE group_id = ? AND member_id = ?",
                (group_id, member_id),
            ).fetchone()
            return _row_to_preference_dict(row)

    # ── Event Dispatch ───────────────────────────────────────────────

    def dispatch_event(
        self,
        payload: NotificationEventCreate,
    ) -> dict[str, Any]:
        """Record a notification event and determine recipients.

        Returns the event record and list of member IDs who should be notified.
        """
        with write_connection(self._db_path) as conn:
            event_id = str(uuid4())
            now = _iso_now()

            conn.execute(
                """
                INSERT INTO notification_events
                    (id, group_id, event_type, triggered_by, payload_json,
                     amount, currency_code, read, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    event_id,
                    payload.group_id,
                    payload.event_type,
                    payload.triggered_by,
                    json.dumps(payload.payload),
                    str(payload.amount) if payload.amount is not None else None,
                    payload.currency_code,
                    now,
                ),
            )

            recipients = self._determine_recipients(
                conn, payload.group_id, payload.event_type,
                payload.triggered_by, payload.amount,
            )

            for member_id in recipients:
                conn.execute(
                    """
                    INSERT INTO notification_deliveries
                        (id, event_id, member_id, delivered, delivered_at, created_at)
                    VALUES (?, ?, ?, 0, NULL, ?)
                    """,
                    (str(uuid4()), event_id, member_id, now),
                )

            return {
                "event_id": event_id,
                "event_type": payload.event_type,
                "group_id": payload.group_id,
                "recipients": recipients,
                "created_at": now,
            }

    def get_events(
        self,
        group_id: str,
        member_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
        unread_only: bool = False,
    ) -> dict[str, Any]:
        """List notification events for a member in a group."""
        with read_connection(self._db_path) as conn:
            base_query = """
                SELECT ne.* FROM notification_events ne
                JOIN notification_deliveries nd ON ne.id = nd.event_id
                WHERE ne.group_id = ? AND nd.member_id = ?
            """
            params: list[Any] = [group_id, member_id]

            if unread_only:
                base_query += " AND ne.read = 0"

            count_row = conn.execute(
                f"SELECT COUNT(*) as cnt FROM ({base_query})", params
            ).fetchone()
            total = count_row["cnt"] if count_row else 0

            rows = conn.execute(
                base_query + " ORDER BY ne.created_at DESC LIMIT ? OFFSET ?",
                params + [limit, offset],
            ).fetchall()

            events = []
            for row in rows:
                events.append({
                    "event_id": row["id"],
                    "event_type": row["event_type"],
                    "triggered_by": row["triggered_by"],
                    "payload": json.loads(row["payload_json"]) if row["payload_json"] else {},
                    "amount": row["amount"],
                    "currency_code": row["currency_code"],
                    "read": bool(row["read"]),
                    "created_at": row["created_at"],
                })

            return {
                "events": events,
                "total": total,
                "limit": limit,
                "offset": offset,
            }

    def mark_events_read(
        self,
        group_id: str,
        member_id: str,
        event_ids: list[str],
    ) -> dict[str, Any]:
        """Mark specific notification events as read."""
        with write_connection(self._db_path) as conn:
            now = _iso_now()
            updated = 0
            for event_id in event_ids:
                result = conn.execute(
                    """
                    UPDATE notification_events SET read = 1
                    WHERE id = ? AND group_id = ?
                    AND id IN (
                        SELECT event_id FROM notification_deliveries
                        WHERE member_id = ?
                    )
                    AND read = 0
                    """,
                    (event_id, group_id, member_id),
                )
                updated += result.rowcount

            return {"marked_read": updated, "total_requested": len(event_ids)}

    # ── Digest ──────────────────────────────────────────────────────

    def compute_digest(
        self,
        group_id: str,
        member_id: str,
        payload: NotificationDigestRequest,
    ) -> dict[str, Any]:
        """Compute a notification digest summary for a member."""
        with read_connection(self._db_path) as conn:
            query = """
                SELECT ne.event_type, COUNT(*) as count,
                       SUM(CASE WHEN ne.amount IS NOT NULL THEN CAST(ne.amount AS REAL) ELSE 0 END) as total_amount,
                       MIN(ne.created_at) as earliest,
                       MAX(ne.created_at) as latest
                FROM notification_events ne
                JOIN notification_deliveries nd ON ne.id = nd.event_id
                WHERE ne.group_id = ? AND nd.member_id = ?
            """
            params: list[Any] = [group_id, member_id]

            if payload.since:
                query += " AND ne.created_at >= ?"
                params.append(ensure_utc(payload.since).isoformat())
            if payload.through:
                query += " AND ne.created_at <= ?"
                params.append(ensure_utc(payload.through).isoformat())
            if not payload.include_read:
                query += " AND ne.read = 0"

            query += " GROUP BY ne.event_type ORDER BY count DESC"
            rows = conn.execute(query, params).fetchall()

            summary_items = []
            total_events = 0
            for row in rows:
                total_events += row["count"]
                summary_items.append({
                    "event_type": row["event_type"],
                    "count": row["count"],
                    "total_amount": row["total_amount"],
                    "earliest": row["earliest"],
                    "latest": row["latest"],
                })

            return {
                "group_id": group_id,
                "member_id": member_id,
                "total_events": total_events,
                "summary": summary_items,
                "period": {
                    "since": payload.since.isoformat() if payload.since else None,
                    "through": payload.through.isoformat() if payload.through else None,
                },
            }

    # ── Internal helpers ─────────────────────────────────────────────

    def _determine_recipients(
        self,
        conn,
        group_id: str,
        event_type: str,
        triggered_by: str,
        amount: Decimal | None,
    ) -> list[str]:
        """Determine which members should receive a notification."""
        rows = conn.execute(
            """
            SELECT member_id, event_types_json, threshold_amount, enabled,
                   quiet_hours_start, quiet_hours_end
            FROM notification_preferences
            WHERE group_id = ? AND enabled = 1
            """,
            (group_id,),
        ).fetchall()

        recipients = []
        current_hour = now_utc().hour

        for row in rows:
            if row["member_id"] == triggered_by:
                continue

            event_types = json.loads(row["event_types_json"])
            if event_type not in event_types:
                continue

            if row["threshold_amount"] is not None and amount is not None:
                threshold = Decimal(row["threshold_amount"])
                if amount < threshold:
                    continue

            if row["quiet_hours_start"] is not None and row["quiet_hours_end"] is not None:
                start_h = row["quiet_hours_start"]
                end_h = row["quiet_hours_end"]
                if _in_quiet_hours(current_hour, start_h, end_h):
                    continue

            recipients.append(row["member_id"])

        return sorted(recipients)

    def _preference_to_dict(
        self, pref_id, group_id, member_id, payload, created_at
    ) -> dict[str, Any]:
        return {
            "id": pref_id,
            "group_id": group_id,
            "member_id": member_id,
            "channel": payload.channel,
            "enabled": payload.enabled,
            "event_types": list(payload.event_types),
            "threshold_amount": str(payload.threshold_amount) if payload.threshold_amount else None,
            "quiet_hours_start": payload.quiet_hours_start,
            "quiet_hours_end": payload.quiet_hours_end,
            "digest_frequency": payload.digest_frequency,
            "created_at": created_at,
        }


def _row_to_preference_dict(row) -> dict[str, Any]:
    """Convert a database row to a preference dict."""
    return {
        "id": row["id"],
        "group_id": row["group_id"],
        "member_id": row["member_id"],
        "channel": row["channel"],
        "enabled": bool(row["enabled"]),
        "event_types": json.loads(row["event_types_json"]),
        "threshold_amount": row["threshold_amount"],
        "quiet_hours_start": row["quiet_hours_start"],
        "quiet_hours_end": row["quiet_hours_end"],
        "digest_frequency": row["digest_frequency"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _in_quiet_hours(current_hour: int, start: int, end: int) -> bool:
    """Check if the current hour falls within quiet hours.

    Handles wrapping around midnight (e.g., start=22, end=6).
    """
    if start <= end:
        return start <= current_hour < end
    return current_hour >= start or current_hour < end


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()
