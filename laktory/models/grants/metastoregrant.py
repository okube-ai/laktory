from typing import Literal
from laktory.models.basemodel import BaseModel


class MetastoreGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a metastore

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
            "CREATE_CATALOG",
            "CREATE_CONNECTION",
            "CREATE_EXTERNAL_LOCATION",
            "CREATE_PROVIDER",
            "CREATE_RECIPIENT",
            "CREATE_SHARE",
            "CREATE_STORAGE_CREDENTIAL",
            "LOCATION",
            "MANAGE_ALLOWLIST",
            "SET_SHARE_PERMISSION",
            "USE_MARKETPLACE_ASSETS",
            "USE_PROVIDER",
            "USE_RECIPIENT",
            "USE_SHARE",
        ]
    ]
