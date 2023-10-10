from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "SELECT",
]


class ViewGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
