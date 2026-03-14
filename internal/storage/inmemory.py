from internal.models.expense import Expense
from internal.models.group import Group

groups: dict[str, Group] = {}
expenses: dict[str, Expense] = {}
balances: dict[str, dict[str, float]] = {}
