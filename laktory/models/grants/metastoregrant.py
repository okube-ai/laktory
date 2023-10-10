from typing import Literal
from laktory.models.base import BaseModel


class MetastoreGrant(BaseModel):
    principal: str
    privileges: list[Literal[
        "CREATE_CATALOG",
        "CREATE_EXTERNAL",
        "LOCATION",
        "CREATE_CONNECTION",
        "CREATE_RECIPIENT",
        "CREATE_SHARE",
        "CREATE_PROVIDER",
        "USE_MARKETPLACE_ASSETS",
        "USE_PROVIDER",
        "USE_SHARE",
        "USE_RECIPIENT",
        "SET_SHARE_PERMISSION",
        "MANAGE_ALLOWLIST",
    ]]
