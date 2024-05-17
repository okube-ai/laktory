from typing import Literal
from laktory.models.basemodel import BaseModel


class VolumeGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a volume

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
            "READ_VOLUME",
            "WRITE_VOLUME",
        ]
    ]
