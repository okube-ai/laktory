from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class ExternalLocationGrant(BaseModel):
    """
    Privileges granted to a principal and operating on an external location

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[
        Literal[
            "ALL_PRIVILEGES",
            "CREATE_EXTERNAL_TABLE",
            "CREATE_EXTERNAL_VOLUME",
            "READ_FILES",
            "WRITE_FILES",
            "CREATE_MANAGED_STORAGE",
        ]
    ] = Field(..., description="List of allowed privileges")
