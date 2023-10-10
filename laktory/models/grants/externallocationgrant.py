from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "CREATE_EXTERNAL_TABLE",
    "CREATE_EXTERNAL_VOLUME",
    "READ_FILES",
    "WRITE_FILES",
    "CREATE_MANAGED_STORAGE",
]


class ExternalLocationGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
