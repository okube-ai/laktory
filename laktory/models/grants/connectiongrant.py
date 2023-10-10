from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "CREATE_FOREIGN_CATALOG",
    "USE_CONNECTION",
]


class ConnectionGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
