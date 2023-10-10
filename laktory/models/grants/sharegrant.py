from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
    "SELECT",
]

class ShareGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
