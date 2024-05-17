from typing import Literal
from laktory.models.basemodel import BaseModel


class CatalogGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a catalog.

    Attributes
    ----------
    principal
        User, group or service principal name
    privileges
        List of allowed privileges

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str
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
    ]
