from typing import Literal
from laktory.models.basemodel import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "EXECUTE",
]


class FunctionGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
