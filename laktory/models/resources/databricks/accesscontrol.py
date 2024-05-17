from typing import Any
from pydantic import model_validator
from laktory.models.basemodel import BaseModel


class AccessControl(BaseModel):
    """
    Databricks Access Control

    Access Control generally applicable to objects like cluster, notebook,
    pipeline, etc. For providing access to securable data, refer to
    `models.Grant` instead.

    Attributes
    ----------
    group_name:
        Name of the group to assign the permission to.
    permission_level:
        Name of the permission to assign
    service_principal_name:
        Name of the service principal to assign the permission to.
    user_name
        Name of the user to assign the permission to.

    Examples
    --------
    ```py
    from laktory import models

    p = models.resources.databricks.AccessControl(
        group_name="role-engineers", permission_level="READ"
    )
    ```
    """

    group_name: str = None
    permission_level: str
    service_principal_name: str = None
    user_name: str = None

    @model_validator(mode="after")
    def single_input(self) -> Any:
        count = 0
        if self.user_name:
            count += 1
        if self.group_name:
            count += 1
        if self.service_principal_name:
            count += 1

        if count == 0:
            raise ValueError(
                "At least one of `user_name`, `group_name`, `service_principal_name` must be specified."
            )
        if count > 1:
            raise ValueError(
                "Ony one of `user_name`, `group_name`, `service_principal_name` must be specified."
            )

        return self
