from typing import Literal
from laktory.models.base import BaseModel
from laktory.models.grants.tablegrant import PRIVILEGES as TABLE_PRIVILEGES
from laktory.models.grants.volumegrant import PRIVILEGES as VOLUME_PRIVILEGES
from laktory.models.grants.functiongrant import PRIVILEGES as FUNCTION_PRIVILEGES
from laktory.models.grants.registeredmodelgrant import PRIVILEGES as MODEL_PRIVILEGES
from laktory.models.grants.viewgrant import PRIVILEGES as VIEWL_PRIVILEGES


PRIVILEGES = TABLE_PRIVILEGES + VOLUME_PRIVILEGES + FUNCTION_PRIVILEGES + MODEL_PRIVILEGES + VIEWL_PRIVILEGES + [
    "ALL_PRIVILEGES",
    "CREATE_FUNCTION",
    "CREATE_MATERIALIZED_VIEW",
    "CREATE_MODEL",
    "CREATE_TABLE",
    "CREATE_VOLUME",
    "USE_SCHEMA",
]


class SchemaGrant(BaseModel):
    principal: str
    privileges: list[Literal[tuple(PRIVILEGES)]]
