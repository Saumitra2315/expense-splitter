"""Schema migration framework for the SettleUp API.

Provides a lightweight migration system that tracks schema versions,
supports forward migrations, and registers built-in migrations for
the notification, budget, and export tables.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Callable


# Type alias for migration functions.
MigrationFn = Callable[[sqlite3.Connection], None]


class MigrationError(Exception):
    """Raised when a migration fails."""
    pass


class MigrationRunner:
    """Manages and executes database schema migrations."""

    def __init__(self):
        self._migrations: list[tuple[int, str, MigrationFn]] = []
        self._register_builtin_migrations()

    def register(self, version: int, description: str, fn: MigrationFn) -> None:
        """Register a new migration.

        Migrations are executed in version order. Each version must be unique.
        """
        existing = {v for v, _, _ in self._migrations}
        if version in existing:
            raise MigrationError(f"Duplicate migration version: {version}")
        self._migrations.append((version, description, fn))
        self._migrations.sort(key=lambda m: m[0])

    def run(self, conn: sqlite3.Connection) -> list[dict[str, str]]:
        """Execute all pending migrations on the given connection.

        Returns a list of applied migration records.
        """
        self._ensure_migration_table(conn)
        current_version = self._current_version(conn)
        applied: list[dict[str, str]] = []

        for version, description, fn in self._migrations:
            if version <= current_version:
                continue

            try:
                fn(conn)
                now = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO schema_migrations (version, description, applied_at)
                    VALUES (?, ?, ?)
                    """,
                    (version, description, now),
                )
                applied.append({
                    "version": str(version),
                    "description": description,
                    "applied_at": now,
                })
            except Exception as exc:
                raise MigrationError(
                    f"Migration v{version} ({description}) failed: {exc}"
                ) from exc

        return applied

    def status(self, conn: sqlite3.Connection) -> dict[str, object]:
        """Get migration status report."""
        self._ensure_migration_table(conn)
        current = self._current_version(conn)
        total = len(self._migrations)
        pending = sum(1 for v, _, _ in self._migrations if v > current)

        applied_rows = conn.execute(
            "SELECT version, description, applied_at FROM schema_migrations ORDER BY version"
        ).fetchall()

        applied = [
            {"version": row[0], "description": row[1], "applied_at": row[2]}
            for row in applied_rows
        ]

        pending_list = [
            {"version": v, "description": d}
            for v, d, _ in self._migrations
            if v > current
        ]

        return {
            "current_version": current,
            "total_migrations": total,
            "pending_count": pending,
            "applied": applied,
            "pending": pending_list,
        }

    def _current_version(self, conn: sqlite3.Connection) -> int:
        """Get the latest applied migration version."""
        row = conn.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()
        return row[0] if row and row[0] is not None else 0

    def _ensure_migration_table(self, conn: sqlite3.Connection) -> None:
        """Create the schema_migrations table if it doesn't exist."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at TEXT NOT NULL
            )
        """)

    def _register_builtin_migrations(self) -> None:
        """Register all built-in migrations for enhanced features."""
        self.register(1, "Add notification tables", _migration_001_notifications)
        self.register(2, "Add budget tables", _migration_002_budgets)
        self.register(3, "Add export tables", _migration_003_exports)
        self.register(4, "Add notification indices", _migration_004_notification_indices)
        self.register(5, "Add budget indices", _migration_005_budget_indices)


# ── Built-in Migrations ─────────────────────────────────────────────

def _migration_001_notifications(conn: sqlite3.Connection) -> None:
    """Create notification preference and event tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS notification_preferences (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            member_id TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'in_app',
            enabled INTEGER NOT NULL DEFAULT 1,
            event_types_json TEXT NOT NULL DEFAULT '[]',
            threshold_amount TEXT,
            quiet_hours_start INTEGER,
            quiet_hours_end INTEGER,
            digest_frequency TEXT NOT NULL DEFAULT 'none',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (group_id, member_id),
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS notification_events (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            triggered_by TEXT NOT NULL,
            payload_json TEXT,
            amount TEXT,
            currency_code TEXT,
            read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS notification_deliveries (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            member_id TEXT NOT NULL,
            delivered INTEGER NOT NULL DEFAULT 0,
            delivered_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (event_id) REFERENCES notification_events(id)
        );
    """)


def _migration_002_budgets(conn: sqlite3.Connection) -> None:
    """Create budget and category rule tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS budgets (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            category TEXT NOT NULL,
            amount TEXT NOT NULL,
            currency_code TEXT NOT NULL,
            period TEXT NOT NULL DEFAULT 'monthly',
            alert_thresholds_json TEXT NOT NULL DEFAULT '[50, 80, 100]',
            rollover INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS category_rules (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            category TEXT NOT NULL,
            keywords_json TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );
    """)


def _migration_003_exports(conn: sqlite3.Connection) -> None:
    """Create export job tracking table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS export_jobs (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            format TEXT NOT NULL DEFAULT 'csv',
            status TEXT NOT NULL DEFAULT 'pending',
            filters_json TEXT,
            columns_json TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            result_json TEXT,
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );
    """)


def _migration_004_notification_indices(conn: sqlite3.Connection) -> None:
    """Add performance indices for notification queries."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_notification_prefs_group
            ON notification_preferences(group_id);
        CREATE INDEX IF NOT EXISTS idx_notification_prefs_member
            ON notification_preferences(group_id, member_id);
        CREATE INDEX IF NOT EXISTS idx_notification_events_group
            ON notification_events(group_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_notification_deliveries_event
            ON notification_deliveries(event_id);
        CREATE INDEX IF NOT EXISTS idx_notification_deliveries_member
            ON notification_deliveries(member_id);
    """)


def _migration_005_budget_indices(conn: sqlite3.Connection) -> None:
    """Add performance indices for budget queries."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_budgets_group
            ON budgets(group_id, active);
        CREATE INDEX IF NOT EXISTS idx_budgets_category
            ON budgets(group_id, category);
        CREATE INDEX IF NOT EXISTS idx_category_rules_group
            ON category_rules(group_id, priority);
        CREATE INDEX IF NOT EXISTS idx_export_jobs_group
            ON export_jobs(group_id, created_at);
    """)
