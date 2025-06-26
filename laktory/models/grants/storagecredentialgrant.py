from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class StorageCredentialGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a storage credential

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[
        Literal[
            "ALL PRIVILEGES",
            "CREATE_EXTERNAL_LOCATION",
            "CREATE_EXTERNAL_TABLE",
            "READ_FILES",
            "WRITE_FILES",
        ]
    ] = Field(..., description="List of allowed privileges")
