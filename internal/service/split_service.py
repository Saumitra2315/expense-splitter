from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Iterable


ZERO_DECIMAL_CURRENCIES = {"JPY", "KRW", "VND"}
THREE_DECIMAL_CURRENCIES = {"BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"}


def currency_scale(currency_code: str) -> int:
    code = currency_code.upper()
    if code in ZERO_DECIMAL_CURRENCIES:
        return 0
    if code in THREE_DECIMAL_CURRENCIES:
        return 3
    return 2


def quantize_money(amount: Decimal, currency_code: str) -> Decimal:
    scale = currency_scale(currency_code)
    quantum = Decimal("1").scaleb(-scale)
    return amount.quantize(quantum)


def compute_allocations(
    *,
    total: Decimal,
    currency_code: str,
    split_mode: str,
    participant_ids: list[str],
    allocations: Iterable[dict[str, Decimal]],
) -> dict[str, Decimal]:
    total = quantize_money(total, currency_code)
    if split_mode == "equal":
        return _equal_allocations(total, currency_code, participant_ids)
    if split_mode == "fixed":
        return _fixed_allocations(total, currency_code, allocations)
    if split_mode == "percentage":
        return _percentage_allocations(total, currency_code, allocations)
    raise ValueError(f"Unsupported split mode: {split_mode}")


def build_transfer_plan(
    balances: dict[str, Decimal],
    currency_code: str,
) -> dict[str, object]:
    scale = currency_scale(currency_code)
    multiplier = 10**scale
    minor_balances = {
        member_id: int((amount * multiplier).to_integral_value())
        for member_id, amount in balances.items()
        if amount != 0
    }
    if len(minor_balances) <= 10:
        transfers = _optimal_transfers(minor_balances, multiplier)
        return {"strategy": "optimal", "transfers": transfers}
    transfers = _greedy_transfers(minor_balances, multiplier)
    return {"strategy": "greedy", "transfers": transfers}


def _equal_allocations(
    total: Decimal,
    currency_code: str,
    participant_ids: list[str],
) -> dict[str, Decimal]:
    if not participant_ids:
        raise ValueError("At least one participant is required")

    ordered = sorted(dict.fromkeys(participant_ids))
    count = len(ordered)
    quantum = Decimal("1").scaleb(-currency_scale(currency_code))
    base = (total / Decimal(count)).quantize(quantum, rounding=ROUND_DOWN)
    allocations = {member_id: base for member_id in ordered}
    remainder = total - (base * count)
    steps = int((remainder / quantum).to_integral_value())

    for member_id in ordered[:steps]:
        allocations[member_id] += quantum

    return allocations


def _fixed_allocations(
    total: Decimal,
    currency_code: str,
    allocations: Iterable[dict[str, Decimal]],
) -> dict[str, Decimal]:
    result = {
        entry["member_id"]: quantize_money(entry["value"], currency_code)
        for entry in allocations
    }
    if quantize_money(sum(result.values(), Decimal("0")), currency_code) != total:
        raise ValueError("Fixed allocations must sum to the total amount")
    return result


def _percentage_allocations(
    total: Decimal,
    currency_code: str,
    allocations: Iterable[dict[str, Decimal]],
) -> dict[str, Decimal]:
    quantum = Decimal("1").scaleb(-currency_scale(currency_code))
    normalized = list(allocations)
    percentage_total = sum(entry["value"] for entry in normalized)
    if percentage_total != Decimal("100"):
        raise ValueError("Percentage allocations must sum to 100")

    ordered = sorted(normalized, key=lambda item: item["member_id"])
    result: dict[str, Decimal] = {}
    allocated = Decimal("0")
    for entry in ordered[:-1]:
        share = (total * entry["value"] / Decimal("100")).quantize(quantum, rounding=ROUND_DOWN)
        result[entry["member_id"]] = share
        allocated += share
    last_member = ordered[-1]["member_id"]
    result[last_member] = total - allocated
    return result


def _optimal_transfers(minor_balances: dict[str, int], multiplier: int) -> list[dict[str, str]]:
    debtors = sorted(
        [(member_id, -amount) for member_id, amount in minor_balances.items() if amount < 0],
        key=lambda item: (-item[1], item[0]),
    )
    creditors = sorted(
        [(member_id, amount) for member_id, amount in minor_balances.items() if amount > 0],
        key=lambda item: (-item[1], item[0]),
    )
    if not debtors or not creditors:
        return []

    debtor_amounts = [amount for _, amount in debtors]
    creditor_amounts = [amount for _, amount in creditors]
    best_plan: list[tuple[str, str, int]] | None = None

    def _lower_bound(start_debtor: int) -> int:
        remaining_debtors = sum(1 for amount in debtor_amounts[start_debtor:] if amount > 0)
        remaining_creditors = sum(1 for amount in creditor_amounts if amount > 0)
        return max(remaining_debtors, remaining_creditors)

    def dfs(start_debtor: int, current_plan: list[tuple[str, str, int]]) -> None:
        nonlocal best_plan

        while start_debtor < len(debtor_amounts) and debtor_amounts[start_debtor] == 0:
            start_debtor += 1
        if start_debtor == len(debtor_amounts):
            if best_plan is None or len(current_plan) < len(best_plan):
                best_plan = list(current_plan)
            return

        if best_plan is not None and len(current_plan) + _lower_bound(start_debtor) >= len(best_plan):
            return

        debt_remaining = debtor_amounts[start_debtor]
        seen_credit_amounts: set[int] = set()

        for creditor_index, credit_amount in enumerate(creditor_amounts):
            if credit_amount == 0 or credit_amount in seen_credit_amounts:
                continue
            seen_credit_amounts.add(credit_amount)

            transfer_minor = min(debt_remaining, credit_amount)
            debtor_amounts[start_debtor] -= transfer_minor
            creditor_amounts[creditor_index] -= transfer_minor
            current_plan.append((debtors[start_debtor][0], creditors[creditor_index][0], transfer_minor))

            next_debtor = start_debtor + 1 if debtor_amounts[start_debtor] == 0 else start_debtor
            dfs(next_debtor, current_plan)

            current_plan.pop()
            debtor_amounts[start_debtor] += transfer_minor
            creditor_amounts[creditor_index] += transfer_minor

    dfs(0, [])
    final_plan = best_plan or []
    return [_transfer_dict(debtor, creditor, amount, multiplier) for debtor, creditor, amount in final_plan]


def _greedy_transfers(minor_balances: dict[str, int], multiplier: int) -> list[dict[str, str]]:
    debtors = sorted(
        [(member_id, -amount) for member_id, amount in minor_balances.items() if amount < 0],
        key=lambda item: item[1],
        reverse=True,
    )
    creditors = sorted(
        [(member_id, amount) for member_id, amount in minor_balances.items() if amount > 0],
        key=lambda item: item[1],
        reverse=True,
    )

    transfers: list[dict[str, str]] = []
    debtor_index = 0
    creditor_index = 0

    while debtor_index < len(debtors) and creditor_index < len(creditors):
        debtor_id, debt_minor = debtors[debtor_index]
        creditor_id, credit_minor = creditors[creditor_index]
        transfer_minor = min(debt_minor, credit_minor)

        transfers.append(_transfer_dict(debtor_id, creditor_id, transfer_minor, multiplier))

        debt_minor -= transfer_minor
        credit_minor -= transfer_minor

        if debt_minor == 0:
            debtor_index += 1
        else:
            debtors[debtor_index] = (debtor_id, debt_minor)

        if credit_minor == 0:
            creditor_index += 1
        else:
            creditors[creditor_index] = (creditor_id, credit_minor)

    return transfers


def _transfer_dict(
    debtor_id: str,
    creditor_id: str,
    amount_minor: int,
    multiplier: int,
) -> dict[str, str]:
    amount = Decimal(amount_minor) / Decimal(multiplier)
    return {
        "from_member_id": debtor_id,
        "to_member_id": creditor_id,
        "amount": format(amount.normalize(), "f") if amount % 1 else format(amount, "f"),
    }
