"""Validation utilities for the SettleUp API.

Provides reusable validators for currency codes, email addresses,
date ranges, monetary amounts, and string sanitization.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


# ISO 4217 currency codes — comprehensive list including major, minor, and exotic currencies.
VALID_CURRENCY_CODES: frozenset[str] = frozenset({
    "AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN",
    "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BRL",
    "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLP", "CNY",
    "COP", "CRC", "CUP", "CVE", "CZK", "DJF", "DKK", "DOP", "DZD", "EGP",
    "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GHS", "GIP", "GMD",
    "GNF", "GTQ", "GYD", "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS",
    "INR", "IQD", "IRR", "ISK", "JMD", "JOD", "JPY", "KES", "KGS", "KHR",
    "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK", "LBP", "LKR", "LRD",
    "LSL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU",
    "MUR", "MVR", "MWK", "MXN", "MYR", "MZN", "NAD", "NGN", "NIO", "NOK",
    "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG",
    "QAR", "RON", "RSD", "RUB", "RWF", "SAR", "SBD", "SCR", "SDG", "SEK",
    "SGD", "SHP", "SLL", "SOS", "SRD", "SSP", "STN", "SVC", "SYP", "SZL",
    "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TWD", "TZS", "UAH",
    "UGX", "USD", "UYU", "UZS", "VES", "VND", "VUV", "WST", "XAF", "XCD",
    "XOF", "XPF", "YER", "ZAR", "ZMW", "ZWL",
})


# Decimal precision limits per type of monetary operation.
MAX_EXPENSE_AMOUNT = Decimal("999999999.99")
MIN_EXPENSE_AMOUNT = Decimal("0.01")
MAX_FX_RATE = Decimal("999999.999999")
MIN_FX_RATE = Decimal("0.000001")


class ValidationResult:
    """Immutable result of a validation check."""

    __slots__ = ("_valid", "_errors")

    def __init__(self, valid: bool, errors: list[str] | None = None) -> None:
        self._valid = valid
        self._errors = errors or []

    @property
    def is_valid(self) -> bool:
        return self._valid

    @property
    def errors(self) -> list[str]:
        return list(self._errors)

    def __bool__(self) -> bool:
        return self._valid

    def __repr__(self) -> str:
        if self._valid:
            return "ValidationResult(valid=True)"
        return f"ValidationResult(valid=False, errors={self._errors!r})"

    @classmethod
    def ok(cls) -> "ValidationResult":
        return cls(valid=True)

    @classmethod
    def fail(cls, *errors: str) -> "ValidationResult":
        return cls(valid=False, errors=list(errors))

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Combine two results — valid only if both are valid."""
        if self._valid and other._valid:
            return ValidationResult.ok()
        combined = self._errors + other._errors
        return ValidationResult.fail(*combined)


def validate_currency_code(code: str) -> ValidationResult:
    """Check that a string is a recognized ISO 4217 currency code."""
    if not isinstance(code, str):
        return ValidationResult.fail(f"Currency code must be a string, got {type(code).__name__}")
    normalized = code.strip().upper()
    if len(normalized) != 3:
        return ValidationResult.fail(f"Currency code must be exactly 3 characters, got {len(normalized)}")
    if not normalized.isalpha():
        return ValidationResult.fail(f"Currency code must contain only letters, got '{normalized}'")
    if normalized not in VALID_CURRENCY_CODES:
        return ValidationResult.fail(f"Unknown currency code: '{normalized}'")
    return ValidationResult.ok()


_EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
)

_DISPLAY_NAME_PATTERN = re.compile(r"^[\w\s.\-']{1,100}$", re.UNICODE)


def validate_email(email: str) -> ValidationResult:
    """Validate an email address format (RFC 5322 simplified)."""
    if not isinstance(email, str):
        return ValidationResult.fail("Email must be a string")
    stripped = email.strip()
    if not stripped:
        return ValidationResult.fail("Email must not be empty")
    if len(stripped) > 254:
        return ValidationResult.fail("Email must not exceed 254 characters")
    local_part, _, domain = stripped.rpartition("@")
    if not local_part or not domain:
        return ValidationResult.fail("Email must contain exactly one '@' character")
    if len(local_part) > 64:
        return ValidationResult.fail("Email local part must not exceed 64 characters")
    if not _EMAIL_PATTERN.match(stripped):
        return ValidationResult.fail(f"Invalid email format: '{stripped}'")
    return ValidationResult.ok()


def validate_display_name(name: str) -> ValidationResult:
    """Validate a member display name."""
    if not isinstance(name, str):
        return ValidationResult.fail("Display name must be a string")
    stripped = name.strip()
    if not stripped:
        return ValidationResult.fail("Display name must not be empty")
    if len(stripped) > 100:
        return ValidationResult.fail("Display name must not exceed 100 characters")
    if not _DISPLAY_NAME_PATTERN.match(stripped):
        return ValidationResult.fail(
            f"Display name contains invalid characters: '{stripped}'"
        )
    return ValidationResult.ok()


def validate_date_range(
    start: datetime | None,
    end: datetime | None,
    *,
    allow_future: bool = True,
    max_span_days: int | None = None,
) -> ValidationResult:
    """Validate a date range for consistency and bounds."""
    errors: list[str] = []

    if start is not None and end is not None:
        start_aware = _ensure_utc(start)
        end_aware = _ensure_utc(end)
        if start_aware >= end_aware:
            errors.append("Start date must be before end date")
        if max_span_days is not None:
            from datetime import timedelta
            if (end_aware - start_aware) > timedelta(days=max_span_days):
                errors.append(f"Date range must not exceed {max_span_days} days")

    if not allow_future:
        now = datetime.now(timezone.utc)
        if start is not None and _ensure_utc(start) > now:
            errors.append("Start date must not be in the future")
        if end is not None and _ensure_utc(end) > now:
            errors.append("End date must not be in the future")

    return ValidationResult.ok() if not errors else ValidationResult.fail(*errors)


def validate_amount(
    amount: Any,
    *,
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
    allow_zero: bool = False,
    allow_negative: bool = False,
    max_decimal_places: int | None = None,
) -> ValidationResult:
    """Validate a monetary amount with configurable constraints."""
    errors: list[str] = []

    try:
        value = Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError):
        return ValidationResult.fail(f"Invalid amount: '{amount}' is not a valid number")

    if value.is_nan() or value.is_infinite():
        return ValidationResult.fail("Amount must be a finite number")

    if not allow_negative and value < 0:
        errors.append("Amount must not be negative")
    if not allow_zero and value == 0:
        errors.append("Amount must not be zero")

    effective_min = min_value if min_value is not None else (None if allow_negative else Decimal("0"))
    if effective_min is not None and value < effective_min:
        errors.append(f"Amount must be at least {effective_min}")

    if max_value is not None and value > max_value:
        errors.append(f"Amount must not exceed {max_value}")

    if max_decimal_places is not None:
        sign, digits, exponent = value.as_tuple()
        actual_places = max(0, -exponent) if isinstance(exponent, int) else 0
        if actual_places > max_decimal_places:
            errors.append(
                f"Amount has {actual_places} decimal places, maximum is {max_decimal_places}"
            )

    return ValidationResult.ok() if not errors else ValidationResult.fail(*errors)


def validate_fx_rate(rate: Any) -> ValidationResult:
    """Validate an FX rate value."""
    result = validate_amount(
        rate,
        min_value=MIN_FX_RATE,
        max_value=MAX_FX_RATE,
        max_decimal_places=6,
    )
    if not result:
        return ValidationResult.fail(
            *(e.replace("Amount", "FX rate") for e in result.errors)
        )
    return result


def validate_expense_amount(amount: Any, currency_code: str = "USD") -> ValidationResult:
    """Validate an expense amount with currency-aware precision."""
    from internal.service.split_service import currency_scale

    scale = currency_scale(currency_code)
    result = validate_amount(
        amount,
        min_value=MIN_EXPENSE_AMOUNT if scale > 0 else Decimal("1"),
        max_value=MAX_EXPENSE_AMOUNT,
        max_decimal_places=scale,
    )
    return result


def sanitize_string(value: str, *, max_length: int = 500) -> str:
    """Sanitize and normalize a user-supplied string.

    - Strips leading/trailing whitespace
    - Collapses internal whitespace runs
    - Truncates to max_length
    - Removes control characters
    """
    if not isinstance(value, str):
        return ""
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_length]


def validate_member_id(member_id: str) -> ValidationResult:
    """Validate a member ID format."""
    if not isinstance(member_id, str):
        return ValidationResult.fail("Member ID must be a string")
    stripped = member_id.strip()
    if not stripped:
        return ValidationResult.fail("Member ID must not be empty")
    if len(stripped) > 100:
        return ValidationResult.fail("Member ID must not exceed 100 characters")
    if not re.match(r"^[a-zA-Z0-9._\-]+$", stripped):
        return ValidationResult.fail(
            f"Member ID must contain only alphanumeric characters, dots, hyphens, underscores"
        )
    return ValidationResult.ok()


def validate_group_name(name: str) -> ValidationResult:
    """Validate a group name."""
    if not isinstance(name, str):
        return ValidationResult.fail("Group name must be a string")
    stripped = name.strip()
    if not stripped:
        return ValidationResult.fail("Group name must not be empty")
    if len(stripped) > 200:
        return ValidationResult.fail("Group name must not exceed 200 characters")
    if len(stripped) < 2:
        return ValidationResult.fail("Group name must be at least 2 characters")
    return ValidationResult.ok()


def _ensure_utc(dt: datetime) -> datetime:
    """Coerce a datetime to UTC. Naive datetimes are assumed UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def validate_pagination(
    offset: int | None = None,
    limit: int | None = None,
    *,
    max_limit: int = 1000,
) -> ValidationResult:
    """Validate pagination parameters."""
    errors: list[str] = []
    if offset is not None and offset < 0:
        errors.append("Offset must not be negative")
    if limit is not None:
        if limit < 1:
            errors.append("Limit must be at least 1")
        if limit > max_limit:
            errors.append(f"Limit must not exceed {max_limit}")
    return ValidationResult.ok() if not errors else ValidationResult.fail(*errors)


def validate_sort_field(
    field: str,
    allowed_fields: set[str],
) -> ValidationResult:
    """Validate that a sort field is in the allowed set."""
    if field not in allowed_fields:
        allowed = ", ".join(sorted(allowed_fields))
        return ValidationResult.fail(
            f"Invalid sort field '{field}'. Allowed: {allowed}"
        )
    return ValidationResult.ok()
