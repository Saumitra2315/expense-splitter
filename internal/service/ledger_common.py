from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def coerce_time(value: datetime | None) -> datetime:
    if value is None:
        return now_utc()
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).astimezone(UTC).replace(microsecond=0)
    return value.astimezone(UTC).replace(microsecond=0)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso(value: datetime) -> str:
    return coerce_time(value).isoformat().replace("+00:00", "Z")


def decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f") if normalized % 1 else format(value, "f")


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def request_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(json_dumps(value).encode("utf-8")).hexdigest()


def advance_time(value: datetime, cadence_unit: str, cadence_count: int) -> datetime:
    if cadence_unit == "day":
        return value + timedelta(days=cadence_count)
    if cadence_unit == "week":
        return value + timedelta(weeks=cadence_count)
    if cadence_unit == "month":
        year = value.year
        month = value.month + cadence_count
        while month > 12:
            year += 1
            month -= 12
        while month < 1:
            year -= 1
            month += 12
        day = min(value.day, _days_in_month(year, month))
        return value.replace(year=year, month=month, day=day)
    raise ValueError(f"Unsupported cadence unit: {cadence_unit}")


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, datetime):
        return iso(value)
    raise TypeError(f"Unsupported type for JSON serialization: {type(value)!r}")


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=UTC)
    this_month = datetime(year, month, 1, tzinfo=UTC)
    return (next_month - this_month).days
