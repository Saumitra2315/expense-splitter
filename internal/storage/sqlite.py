from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "expense_splitter.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    base_currency TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    active INTEGER NOT NULL,
    joined_at TEXT,
    left_at TEXT,
    PRIMARY KEY (group_id, member_id),
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE TABLE IF NOT EXISTS membership_events (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    action TEXT NOT NULL,
    display_name TEXT,
    effective_at TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE TABLE IF NOT EXISTS fx_rates (
    id TEXT PRIMARY KEY,
    base_currency TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    rate TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    source TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS expenses (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    paid_by TEXT NOT NULL,
    amount TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    description TEXT,
    split_mode TEXT NOT NULL,
    split_payload TEXT NOT NULL,
    recurring_template_id TEXT,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_recurring_occurrence
ON expenses(recurring_template_id, occurred_at)
WHERE recurring_template_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS expense_allocations (
    expense_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    amount TEXT NOT NULL,
    PRIMARY KEY (expense_id, member_id),
    FOREIGN KEY (expense_id) REFERENCES expenses(id)
);

CREATE TABLE IF NOT EXISTS recurring_templates (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    paid_by TEXT NOT NULL,
    amount TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    description TEXT,
    split_mode TEXT NOT NULL,
    split_payload TEXT NOT NULL,
    cadence_unit TEXT NOT NULL,
    cadence_count INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    next_run_at TEXT NOT NULL,
    ends_at TEXT,
    active INTEGER NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE TABLE IF NOT EXISTS settlements (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    paid_by TEXT NOT NULL,
    received_by TEXT NOT NULL,
    amount TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    description TEXT,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    member_from TEXT NOT NULL,
    member_to TEXT NOT NULL,
    amount TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    group_version INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

CREATE TABLE IF NOT EXISTS idempotency_records (
    scope TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_operations (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    client_operation_id TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    group_id TEXT,
    expected_group_version INTEGER,
    status TEXT NOT NULL,
    result_json TEXT,
    conflict_json TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (device_id, client_operation_id)
);

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

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

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
CREATE INDEX IF NOT EXISTS idx_budgets_group
    ON budgets(group_id, active);
CREATE INDEX IF NOT EXISTS idx_budgets_category
    ON budgets(group_id, category);
CREATE INDEX IF NOT EXISTS idx_category_rules_group
    ON category_rules(group_id, priority);
CREATE INDEX IF NOT EXISTS idx_export_jobs_group
    ON export_jobs(group_id, created_at);
"""


def resolve_db_path() -> Path:
    configured = os.getenv("EXPENSE_SPLITTER_DB_PATH")
    if configured:
        return Path(configured)
    return DEFAULT_DB_PATH


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or resolve_db_path()
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(db_path: Path | None = None) -> Path:
    path = db_path or resolve_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
    return path


@contextmanager
def read_connection(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_connection(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
