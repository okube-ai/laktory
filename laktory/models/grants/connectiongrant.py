from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class ConnectionGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a connection

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[
        Literal[
            "ALL_PRIVILEGES",
            "CREATE_FOREIGN_CATALOG",
            "USE_CONNECTION",
        ]
    ] = Field(..., description="List of allowed privileges")
