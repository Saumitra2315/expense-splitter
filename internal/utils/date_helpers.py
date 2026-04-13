"""Date and time helper utilities for the SettleUp API.

Provides date range iteration, period boundary calculations,
business day logic, and timezone-aware date coercion.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, date
from typing import Iterator, Literal


PeriodUnit = Literal["day", "week", "month", "quarter", "year"]


def iterate_date_range(
    start: date,
    end: date,
    *,
    step_unit: PeriodUnit = "day",
    step_count: int = 1,
    inclusive_end: bool = True,
) -> Iterator[date]:
    """Yield dates from start to end at the given cadence.

    Args:
        start: First date in the range.
        end: Last date (inclusive or exclusive per ``inclusive_end``).
        step_unit: Unit of iteration (``day``, ``week``, ``month``).
        step_count: Number of units per step.
        inclusive_end: Whether to include the end date.

    Yields:
        Successive dates in the range.
    """
    if step_count < 1:
        raise ValueError("step_count must be at least 1")

    current = start
    boundary = end if inclusive_end else end - timedelta(days=1)

    while current <= boundary:
        yield current
        current = _advance_date(current, step_unit, step_count)


def iterate_datetime_range(
    start: datetime,
    end: datetime,
    *,
    step_unit: PeriodUnit = "day",
    step_count: int = 1,
    inclusive_end: bool = True,
) -> Iterator[datetime]:
    """Yield datetimes from start to end at the given cadence.

    Similar to iterate_date_range but works with full datetimes,
    preserving the time-of-day component.
    """
    if step_count < 1:
        raise ValueError("step_count must be at least 1")

    current = start
    compare_end = end if inclusive_end else end - timedelta(microseconds=1)

    while current <= compare_end:
        yield current
        next_date = _advance_date(current.date(), step_unit, step_count)
        current = current.replace(
            year=next_date.year,
            month=next_date.month,
            day=next_date.day,
        )


def period_start(dt: date | datetime, unit: PeriodUnit) -> date:
    """Return the first day of the period containing ``dt``.

    Args:
        dt: A date or datetime.
        unit: Period granularity.

    Returns:
        The start date of the containing period.
    """
    d = dt if isinstance(dt, date) and not isinstance(dt, datetime) else (dt.date() if isinstance(dt, datetime) else dt)

    if unit == "day":
        return d
    if unit == "week":
        return d - timedelta(days=d.weekday())
    if unit == "month":
        return d.replace(day=1)
    if unit == "quarter":
        quarter_month = ((d.month - 1) // 3) * 3 + 1
        return d.replace(month=quarter_month, day=1)
    if unit == "year":
        return d.replace(month=1, day=1)
    raise ValueError(f"Unsupported period unit: {unit}")


def period_end(dt: date | datetime, unit: PeriodUnit) -> date:
    """Return the last day of the period containing ``dt``.

    Args:
        dt: A date or datetime.
        unit: Period granularity.

    Returns:
        The end date (inclusive) of the containing period.
    """
    d = dt if isinstance(dt, date) and not isinstance(dt, datetime) else (dt.date() if isinstance(dt, datetime) else dt)

    if unit == "day":
        return d
    if unit == "week":
        return d + timedelta(days=6 - d.weekday())
    if unit == "month":
        return _last_day_of_month(d.year, d.month)
    if unit == "quarter":
        quarter_end_month = ((d.month - 1) // 3) * 3 + 3
        return _last_day_of_month(d.year, quarter_end_month)
    if unit == "year":
        return d.replace(month=12, day=31)
    raise ValueError(f"Unsupported period unit: {unit}")


def next_period_start(dt: date | datetime, unit: PeriodUnit) -> date:
    """Return the first day of the next period after the one containing ``dt``."""
    end = period_end(dt, unit)
    return end + timedelta(days=1)


def count_periods_between(
    start: date,
    end: date,
    unit: PeriodUnit,
) -> int:
    """Count the number of complete periods between two dates.

    Returns the number of full periods that fit between start and end.
    """
    if start >= end:
        return 0

    count = 0
    current = period_start(start, unit)
    while current < end:
        current = next_period_start(current, unit)
        if current <= end:
            count += 1

    return count


def is_business_day(d: date) -> bool:
    """Check if a date is a business day (Monday through Friday).

    Note: Does not account for public holidays.
    """
    return d.weekday() < 5


def next_business_day(d: date) -> date:
    """Return the next business day on or after the given date."""
    current = d
    while not is_business_day(current):
        current += timedelta(days=1)
    return current


def previous_business_day(d: date) -> date:
    """Return the most recent business day on or before the given date."""
    current = d
    while not is_business_day(current):
        current -= timedelta(days=1)
    return current


def business_days_between(start: date, end: date) -> int:
    """Count business days between two dates (exclusive of end)."""
    if start >= end:
        return 0

    count = 0
    current = start
    while current < end:
        if is_business_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def ensure_utc(dt: datetime) -> datetime:
    """Coerce a datetime to UTC timezone.

    Naive datetimes are assumed to already be in UTC and are simply
    annotated. Aware datetimes are converted.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ensure_aware(dt: datetime, tz: timezone = timezone.utc) -> datetime:
    """Ensure a datetime is timezone-aware, defaulting to ``tz``."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt


def now_utc() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def start_of_day(dt: datetime) -> datetime:
    """Return midnight (00:00:00) of the given date in the same timezone."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime) -> datetime:
    """Return 23:59:59.999999 of the given date in the same timezone."""
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def days_ago(n: int) -> datetime:
    """Return the datetime ``n`` days before now (UTC)."""
    return now_utc() - timedelta(days=n)


def days_from_now(n: int) -> datetime:
    """Return the datetime ``n`` days after now (UTC)."""
    return now_utc() + timedelta(days=n)


def parse_iso_datetime(text: str) -> datetime:
    """Parse an ISO 8601 datetime string to a timezone-aware datetime.

    Handles formats with and without timezone information.
    Naive results are treated as UTC.
    """
    text = text.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Cannot parse datetime: '{text}'") from exc
    return ensure_utc(dt)


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string.

    Examples: "1.2s", "45.0s", "2m 30s", "1h 15m", "2d 3h"
    """
    if seconds < 0:
        return f"-{format_duration(-seconds)}"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    remaining_secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {remaining_secs}s"
    hours = minutes // 60
    remaining_mins = minutes % 60
    if hours < 24:
        return f"{hours}h {remaining_mins}m"
    days = hours // 24
    remaining_hours = hours % 24
    return f"{days}d {remaining_hours}h"


def _advance_date(d: date, unit: PeriodUnit, count: int) -> date:
    """Advance a date by ``count`` units of ``unit``."""
    if unit == "day":
        return d + timedelta(days=count)
    if unit == "week":
        return d + timedelta(weeks=count)
    if unit == "month":
        month = d.month - 1 + count
        year = d.year + month // 12
        month = month % 12 + 1
        day = min(d.day, _days_in_month(year, month))
        return d.replace(year=year, month=month, day=day)
    if unit == "quarter":
        return _advance_date(d, "month", count * 3)
    if unit == "year":
        try:
            return d.replace(year=d.year + count)
        except ValueError:
            # Handle Feb 29 in non-leap years
            return d.replace(year=d.year + count, day=28)
    raise ValueError(f"Unsupported period unit: {unit}")


def _days_in_month(year: int, month: int) -> int:
    """Return the number of days in a given month/year."""
    import calendar
    return calendar.monthrange(year, month)[1]


def _last_day_of_month(year: int, month: int) -> date:
    """Return the last day of a given month/year."""
    return date(year, month, _days_in_month(year, month))
