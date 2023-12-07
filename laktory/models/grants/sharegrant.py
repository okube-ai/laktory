from typing import Literal
from laktory.models.basemodel import BaseModel

PRIVILEGES = [
    "SELECT",
]


class ShareGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
