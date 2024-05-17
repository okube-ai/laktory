from typing import Literal
from laktory.models.basemodel import BaseModel


class FunctionGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a function

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
    privileges: list[Literal["ALL_PRIVILEGES", "EXECUTE"]]
