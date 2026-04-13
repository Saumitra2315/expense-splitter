"""Budget models for the SettleUp API.

Defines Pydantic schemas for budget management, category rules,
and budget alert tracking.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


BudgetPeriod = Literal["weekly", "monthly", "quarterly", "yearly"]


class CategoryRule(BaseModel):
    """Rule for auto-categorizing expenses based on description keywords."""

    model_config = ConfigDict(extra="forbid")

    category: str = Field(..., min_length=1, max_length=50)
    keywords: list[str] = Field(
        ...,
        min_length=1,
        description="Keywords to match in expense descriptions (case-insensitive)",
    )
    priority: int = Field(
        default=0,
        ge=0,
        le=100,
        description="Higher priority rules are checked first",
    )

    @field_validator("keywords")
    @classmethod
    def normalize_keywords(cls, values: list[str]) -> list[str]:
        return [kw.lower().strip() for kw in values if kw.strip()]

    @field_validator("category")
    @classmethod
    def normalize_category(cls, value: str) -> str:
        return value.strip().lower()


class BudgetCreate(BaseModel):
    """Schema for creating a new budget."""

    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(..., min_length=1)
    category: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Budget category (e.g., 'food', 'transport', 'entertainment')",
    )
    amount: Decimal = Field(..., gt=0, description="Budget limit amount")
    currency_code: str = Field(..., min_length=3, max_length=3)
    period: BudgetPeriod = Field(
        default="monthly",
        description="Budget period for reset cycle",
    )
    alert_thresholds: list[int] = Field(
        default_factory=lambda: [50, 80, 100],
        description="Percentage thresholds at which to trigger alerts",
    )
    rollover: bool = Field(
        default=False,
        description="Whether unused budget rolls over to next period",
    )
    notes: str | None = Field(
        default=None,
        max_length=500,
        description="Optional notes for this budget",
    )

    @field_validator("currency_code")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()

    @field_validator("category")
    @classmethod
    def normalize_category(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("alert_thresholds")
    @classmethod
    def validate_thresholds(cls, values: list[int]) -> list[int]:
        for pct in values:
            if pct < 1 or pct > 200:
                raise ValueError(f"Alert threshold must be between 1 and 200, got {pct}")
        return sorted(set(values))


class BudgetUpdate(BaseModel):
    """Schema for updating an existing budget."""

    model_config = ConfigDict(extra="forbid")

    amount: Decimal | None = Field(default=None, gt=0)
    period: BudgetPeriod | None = None
    alert_thresholds: list[int] | None = None
    rollover: bool | None = None
    notes: str | None = Field(default=None, max_length=500)

    @field_validator("alert_thresholds")
    @classmethod
    def validate_thresholds(cls, values: list[int] | None) -> list[int] | None:
        if values is None:
            return None
        for pct in values:
            if pct < 1 or pct > 200:
                raise ValueError(f"Alert threshold must be between 1 and 200, got {pct}")
        return sorted(set(values))


class CategoryRuleSet(BaseModel):
    """Schema for creating or replacing category rules for a group."""

    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(..., min_length=1)
    rules: list[CategoryRule] = Field(
        default_factory=list,
        description="Ordered list of category rules",
    )

    @model_validator(mode="after")
    def validate_no_duplicate_categories(self) -> "CategoryRuleSet":
        categories = [r.category for r in self.rules]
        if len(categories) != len(set(categories)):
            raise ValueError("Duplicate category names in rules")
        return self


# Default category rules used when no custom rules are defined.
DEFAULT_CATEGORY_RULES: list[dict[str, object]] = [
    {
        "category": "food",
        "keywords": [
            "restaurant", "dinner", "lunch", "breakfast", "cafe", "coffee",
            "pizza", "sushi", "burger", "groceries", "supermarket", "food",
            "bar", "pub", "drinks", "brunch", "takeout", "delivery",
        ],
        "priority": 10,
    },
    {
        "category": "transport",
        "keywords": [
            "taxi", "uber", "lyft", "bus", "train", "metro", "subway",
            "fuel", "gas", "petrol", "parking", "toll", "flight", "airline",
            "car rental", "rental car", "ride", "transit",
        ],
        "priority": 10,
    },
    {
        "category": "accommodation",
        "keywords": [
            "hotel", "airbnb", "hostel", "motel", "lodge", "resort",
            "accommodation", "stay", "room", "apartment", "villa", "rent",
        ],
        "priority": 10,
    },
    {
        "category": "entertainment",
        "keywords": [
            "movie", "cinema", "theater", "theatre", "concert", "show",
            "museum", "gallery", "park", "game", "sport", "ticket",
            "festival", "event", "club", "karaoke",
        ],
        "priority": 5,
    },
    {
        "category": "shopping",
        "keywords": [
            "shop", "store", "mall", "clothes", "electronics", "gift",
            "souvenir", "book", "supplies", "market", "purchase", "buy",
        ],
        "priority": 5,
    },
    {
        "category": "utilities",
        "keywords": [
            "phone", "internet", "wifi", "electricity", "water", "gas",
            "utility", "subscription", "bill", "insurance", "maintenance",
        ],
        "priority": 3,
    },
    {
        "category": "health",
        "keywords": [
            "doctor", "hospital", "pharmacy", "medicine", "medical",
            "health", "dental", "gym", "fitness", "wellness",
        ],
        "priority": 3,
    },
    {
        "category": "other",
        "keywords": [],
        "priority": 0,
    },
]
