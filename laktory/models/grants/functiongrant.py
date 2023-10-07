from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "EXECUTE",
]


class FunctionGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
