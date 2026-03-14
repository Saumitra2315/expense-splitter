"""Formatting utilities for the Expense Splitter API.

Handles money formatting, date/time display, and structured output
generation for exports and API responses.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any


# Currency symbol mapping for display purposes.
CURRENCY_SYMBOLS: dict[str, str] = {
    "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CNY": "¥",
    "KRW": "₩", "INR": "₹", "RUB": "₽", "TRY": "₺", "BRL": "R$",
    "ZAR": "R", "CHF": "CHF", "SEK": "kr", "NOK": "kr", "DKK": "kr",
    "PLN": "zł", "CZK": "Kč", "HUF": "Ft", "THB": "฿", "MYR": "RM",
    "SGD": "S$", "HKD": "HK$", "TWD": "NT$", "AUD": "A$", "NZD": "NZ$",
    "CAD": "C$", "MXN": "MX$", "ARS": "AR$", "COP": "CO$", "PEN": "S/",
    "CLP": "CL$", "PHP": "₱", "IDR": "Rp", "VND": "₫", "ILS": "₪",
    "AED": "د.إ", "SAR": "﷼", "EGP": "E£", "NGN": "₦", "KES": "KSh",
    "GHS": "GH₵", "PKR": "₨", "BDT": "৳", "LKR": "Rs", "NPR": "Rs",
}

# Grouping styles: (thousands_sep, decimal_sep).
LOCALE_GROUPING: dict[str, tuple[str, str]] = {
    "USD": (",", "."), "EUR": (".", ","), "GBP": (",", "."),
    "JPY": (",", "."), "CHF": ("'", "."), "BRL": (".", ","),
    "SEK": (" ", ","), "NOK": (" ", ","), "DKK": (".", ","),
    "PLN": (" ", ","), "CZK": (" ", ","), "HUF": (" ", ","),
    "TRY": (".", ","), "RUB": (" ", ","),
}

DEFAULT_GROUPING: tuple[str, str] = (",", ".")


def format_money(
    amount: Decimal,
    currency_code: str,
    *,
    show_symbol: bool = True,
    show_code: bool = False,
    force_sign: bool = False,
) -> str:
    """Format a monetary amount with the appropriate currency symbol and grouping.

    Args:
        amount: The decimal amount to format.
        currency_code: ISO 4217 currency code.
        show_symbol: Whether to prepend the currency symbol.
        show_code: Whether to append the currency code.
        force_sign: Whether to always show the sign (+ or -).

    Returns:
        Formatted money string, e.g. "$1,234.56" or "€1.234,56".
    """
    from internal.service.split_service import currency_scale

    code = currency_code.upper()
    scale = currency_scale(code)
    thousands_sep, decimal_sep = LOCALE_GROUPING.get(code, DEFAULT_GROUPING)

    abs_amount = abs(amount)
    quantum = Decimal(10) ** -scale
    quantized = abs_amount.quantize(quantum)

    int_part = int(quantized)
    frac_part = quantized - int_part

    int_str = _group_digits(str(int_part), thousands_sep)

    if scale > 0:
        frac_str = str(frac_part)[2:].ljust(scale, "0")
        formatted = f"{int_str}{decimal_sep}{frac_str}"
    else:
        formatted = int_str

    sign = ""
    if amount < 0:
        sign = "-"
    elif force_sign and amount > 0:
        sign = "+"

    parts: list[str] = []
    if show_symbol:
        symbol = CURRENCY_SYMBOLS.get(code, "")
        if symbol:
            parts.append(f"{sign}{symbol}{formatted}")
        else:
            parts.append(f"{sign}{formatted}")
    else:
        parts.append(f"{sign}{formatted}")

    if show_code:
        parts.append(code)

    return " ".join(parts)


def _group_digits(digits: str, separator: str) -> str:
    """Insert thousands separators into a digit string."""
    if len(digits) <= 3:
        return digits
    groups: list[str] = []
    while len(digits) > 3:
        groups.append(digits[-3:])
        digits = digits[:-3]
    groups.append(digits)
    return separator.join(reversed(groups))


def format_datetime_iso(dt: datetime) -> str:
    """Format a datetime as ISO 8601 string in UTC."""
    utc_dt = _to_utc(dt)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_datetime_human(dt: datetime) -> str:
    """Format a datetime in human-readable form."""
    utc_dt = _to_utc(dt)
    return utc_dt.strftime("%b %d, %Y at %I:%M %p UTC")


def format_datetime_relative(dt: datetime, *, now: datetime | None = None) -> str:
    """Format a datetime as a relative time string (e.g., '2 hours ago').

    Supports past and future dates with appropriate phrasing.
    """
    reference = now or datetime.now(timezone.utc)
    utc_dt = _to_utc(dt)
    utc_ref = _to_utc(reference)
    delta = utc_ref - utc_dt

    if delta < timedelta(0):
        return _format_future_delta(-delta)

    seconds = int(delta.total_seconds())
    if seconds < 30:
        return "just now"
    if seconds < 60:
        return f"{seconds} seconds ago"

    minutes = seconds // 60
    if minutes == 1:
        return "1 minute ago"
    if minutes < 60:
        return f"{minutes} minutes ago"

    hours = minutes // 60
    if hours == 1:
        return "1 hour ago"
    if hours < 24:
        return f"{hours} hours ago"

    days = hours // 24
    if days == 1:
        return "yesterday"
    if days < 7:
        return f"{days} days ago"

    weeks = days // 7
    if weeks == 1:
        return "1 week ago"
    if weeks < 4:
        return f"{weeks} weeks ago"

    months = days // 30
    if months == 1:
        return "1 month ago"
    if months < 12:
        return f"{months} months ago"

    years = days // 365
    if years == 1:
        return "1 year ago"
    return f"{years} years ago"


def _format_future_delta(delta: timedelta) -> str:
    """Format a future time delta."""
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return f"in {seconds} seconds"
    minutes = seconds // 60
    if minutes < 60:
        return f"in {minutes} minute{'s' if minutes > 1 else ''}"
    hours = minutes // 60
    if hours < 24:
        return f"in {hours} hour{'s' if hours > 1 else ''}"
    days = hours // 24
    if days < 30:
        return f"in {days} day{'s' if days > 1 else ''}"
    months = days // 30
    if months < 12:
        return f"in {months} month{'s' if months > 1 else ''}"
    years = days // 365
    return f"in {years} year{'s' if years > 1 else ''}"


def format_balance_summary(
    balances: list[dict[str, Any]],
    currency_code: str,
) -> str:
    """Generate a human-readable text summary of balances.

    Args:
        balances: List of dicts with 'member_id', 'display_name', 'net_amount'.
        currency_code: The currency code for formatting.

    Returns:
        Multi-line summary text.
    """
    if not balances:
        return "No balances to display."

    lines = ["Balance Summary:", ""]

    owes: list[str] = []
    owed: list[str] = []
    settled: list[str] = []

    for entry in sorted(balances, key=lambda e: Decimal(str(e.get("net_amount", "0")))):
        name = entry.get("display_name", entry.get("member_id", "Unknown"))
        amount = Decimal(str(entry.get("net_amount", "0")))
        formatted = format_money(abs(amount), currency_code, show_symbol=True)

        if amount < 0:
            owes.append(f"  {name} owes {formatted}")
        elif amount > 0:
            owed.append(f"  {name} is owed {formatted}")
        else:
            settled.append(f"  {name} is settled up")

    if owes:
        lines.append("Members who owe:")
        lines.extend(owes)
        lines.append("")
    if owed:
        lines.append("Members who are owed:")
        lines.extend(owed)
        lines.append("")
    if settled:
        lines.append("Settled:")
        lines.extend(settled)
        lines.append("")

    return "\n".join(lines)


def format_transfer_plan(
    transfers: list[dict[str, str]],
    currency_code: str,
    member_names: dict[str, str] | None = None,
) -> str:
    """Generate a human-readable settlement plan.

    Args:
        transfers: List of dicts with 'from_member_id', 'to_member_id', 'amount'.
        currency_code: The currency for formatting amounts.
        member_names: Optional mapping of member_id -> display_name.

    Returns:
        Multi-line plan text.
    """
    if not transfers:
        return "Everyone is settled up! No transfers needed."

    names = member_names or {}
    lines = [f"Settlement Plan ({len(transfers)} transfer{'s' if len(transfers) != 1 else ''}):", ""]

    for i, transfer in enumerate(transfers, start=1):
        from_id = transfer["from_member_id"]
        to_id = transfer["to_member_id"]
        amount = Decimal(str(transfer["amount"]))
        from_name = names.get(from_id, from_id)
        to_name = names.get(to_id, to_id)
        formatted = format_money(amount, currency_code, show_symbol=True)
        lines.append(f"  {i}. {from_name} → {to_name}: {formatted}")

    return "\n".join(lines)


def format_csv_row(
    values: list[Any],
    *,
    delimiter: str = ",",
    quote_char: str = '"',
) -> str:
    """Format a list of values as a CSV row with proper quoting.

    Handles values containing delimiters, newlines, and quote characters.
    """
    cells: list[str] = []
    for value in values:
        text = str(value) if value is not None else ""
        needs_quoting = (
            delimiter in text
            or "\n" in text
            or "\r" in text
            or quote_char in text
        )
        if needs_quoting:
            escaped = text.replace(quote_char, quote_char + quote_char)
            cells.append(f"{quote_char}{escaped}{quote_char}")
        else:
            cells.append(text)
    return delimiter.join(cells)


def format_expense_description(
    paid_by_name: str,
    amount: Decimal,
    currency_code: str,
    description: str | None,
    participant_count: int,
) -> str:
    """Generate a descriptive one-line summary of an expense.

    Example: 'Alice paid $45.00 for "Dinner" (split 3 ways)'
    """
    formatted_amount = format_money(amount, currency_code, show_symbol=True)
    desc_part = f' for "{description}"' if description else ""
    split_part = f" (split {participant_count} way{'s' if participant_count > 1 else ''})"
    return f"{paid_by_name} paid {formatted_amount}{desc_part}{split_part}"


def format_audit_entry(
    event_type: str,
    entity_type: str,
    occurred_at: datetime,
    payload: dict[str, Any],
) -> str:
    """Format a single audit log entry as a readable line."""
    time_str = format_datetime_iso(occurred_at)
    summary = _summarize_payload(event_type, entity_type, payload)
    return f"[{time_str}] {event_type.upper()} {entity_type}: {summary}"


def _summarize_payload(
    event_type: str,
    entity_type: str,
    payload: dict[str, Any],
) -> str:
    """Create a brief textual summary of an audit event payload."""
    if entity_type == "expense":
        amount = payload.get("amount", "?")
        currency = payload.get("currency_code", "?")
        desc = payload.get("description", "")
        return f"amount={amount} {currency}" + (f' "{desc}"' if desc else "")
    if entity_type == "group":
        name = payload.get("name", "?")
        return f'name="{name}"'
    if entity_type == "settlement":
        amount = payload.get("amount", "?")
        currency = payload.get("currency_code", "?")
        paid_by = payload.get("paid_by", "?")
        received_by = payload.get("received_by", "?")
        return f"{paid_by} → {received_by} amount={amount} {currency}"
    if entity_type == "member":
        action = payload.get("action", event_type)
        member_id = payload.get("member_id", "?")
        return f"{action} member_id={member_id}"
    return str(payload)[:100]


def _to_utc(dt: datetime) -> datetime:
    """Ensure a datetime is in UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
