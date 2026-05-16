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
            "BROWSE",
            "CREATE_EXTERNAL_TABLE",
            "CREATE_EXTERNAL_VOLUME",
            "CREATE_FOREIGN_SECURABLE",
            "CREATE_MANAGED_STORAGE",
            "EXTERNAL_USE_LOCATION",
            "MANAGE",
            "READ_FILES",
            "WRITE_FILES",
        ]
    ] = Field(..., description="List of allowed privileges")
