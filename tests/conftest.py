from __future__ import annotations

import pytest

from internal.service.budget_service import BudgetService
from internal.service.ledger_service import LedgerService


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    path = tmp_path / "settleup_test.db"
    monkeypatch.setenv("SETTLEUP_DB_PATH", str(path))
    return path


@pytest.fixture
def ledger_service(db_path) -> LedgerService:
    return LedgerService()


@pytest.fixture
def budget_service(db_path) -> BudgetService:
    return BudgetService()
