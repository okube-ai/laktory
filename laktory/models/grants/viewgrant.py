from typing import Literal
from laktory.models.basemodel import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "SELECT",
]


class ViewGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
