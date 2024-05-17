from typing import Literal
from laktory.models.basemodel import BaseModel


class ExternalLocationGrant(BaseModel):
    """
    Privileges granted to a principal and operating on an external location

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
            "CREATE_EXTERNAL_TABLE",
            "CREATE_EXTERNAL_VOLUME",
            "READ_FILES",
            "WRITE_FILES",
            "CREATE_MANAGED_STORAGE",
        ]
    ]
