from pydantic import BaseModel, Field


class ExpenseCreate(BaseModel):
    group_id: str
    paid_by: str
    amount: float = Field(..., gt=0)
    split_between: list[str] = Field(default_factory=list)
    description: str | None = None


class Expense(BaseModel):
    id: str
    group_id: str
    paid_by: str
    amount: float
    split_between: list[str]
    description: str | None = None
