from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel


class RegisteredModelGrant(BaseModel):
    """
    Privileges granted to a principal and operating on a registered model

    References
    ----------
    * [privilege types](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html#privilege-types)
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[
        Literal[
            "ALL_PRIVILEGES",
            "APPLY_TAG",
            "CREATE_MODEL_VERSION",
            "EXECUTE",
            "MANAGE",
        ]
    ] = Field(..., description="List of allowed privileges")
