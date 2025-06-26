from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class ShareGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a share

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[Literal["SELECT"]] = Field(
        ..., description="List of allowed privileges"
    )
