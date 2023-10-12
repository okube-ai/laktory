from typing import Any
from pydantic import model_validator
from laktory.models.base import BaseModel


class Permission(BaseModel):
    permission_level: str
    user_name: str = None
    group_name: str = None
    service_principal_name: str = None

    @model_validator(mode="after")
    def singlue_input(self) -> Any:

        count = 0
        if self.user_name:
            count += 1
        if self.group_name:
            count += 1
        if self.service_principal_name:
            count += 1

        if count == 0:
            raise ValueError("At least one of `user_name`, `group_name`, `service_principal_name` must be specified.")
        if count > 1:
            raise ValueError("Ony one of `user_name`, `group_name`, `service_principal_name` must be specified.")

        return self
