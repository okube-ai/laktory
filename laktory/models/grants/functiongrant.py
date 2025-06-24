from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class FunctionGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a function

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[Literal["ALL_PRIVILEGES", "EXECUTE"]] = Field(
        ..., description="List of allowed privileges"
    )
