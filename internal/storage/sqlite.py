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
