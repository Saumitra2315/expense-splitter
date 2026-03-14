from pydantic import BaseModel, Field


class GroupCreate(BaseModel):
    name: str = Field(..., min_length=1)
    members: list[str] = Field(default_factory=list)


class Group(BaseModel):
    id: str
    name: str
    members: list[str]
