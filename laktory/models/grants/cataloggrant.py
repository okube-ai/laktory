from typing import Literal
from laktory.models.base import BaseModel
from laktory.models.grants.schemagrant import PRIVILEGES as DB_PRIVILEGES

PRIVILEGES = DB_PRIVILEGES + [
    "USE_CATALOG",
    "CREATE_SCHEMA",
]


class CatalogGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
