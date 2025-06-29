from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel

PRIVILEGES = [
    "ALL_PRIVILEGES",
    "SELECT",
]


class ViewGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a view

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[
        Literal[
            "ALL_PRIVILEGES",
            "SELECT",
        ]
    ] = Field(..., description="List of allowed privileges")
