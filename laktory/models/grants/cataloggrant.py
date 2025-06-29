from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class CatalogGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a catalog.

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[
        Literal[
            "ALL_PRIVILEGES",
            "CREATE_FUNCTION",
            "CREATE_MATERIALIZED_VIEW",
            "CREATE_MODEL",
            "CREATE_SCHEMA",
            "CREATE_TABLE",
            "CREATE_VOLUME",
            "EXECUTE",
            "MODIFY",
            "READ_VOLUME",
            "SELECT",
            "USE_CATALOG",
            "USE_SCHEMA",
            "WRITE_VOLUME",
        ]
    ] = Field(..., description="List of allowed privileges")
