from typing import Literal
from laktory.models.base import BaseModel

PRIVILEGES = [
        "ALL_PRIVILEGES",
        "READ_VOLUME",
        "WRITE_VOLUME",
    ]


class VolumeGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
