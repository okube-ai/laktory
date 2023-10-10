from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
    "ALL PRIVILEGES",
    "CREATE_EXTERNAL_LOCATION",
    "CREATE_EXTERNAL_TABLE",
    "READ_FILES",
    "WRITE_FILES",
]


class StorageCredentialGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
