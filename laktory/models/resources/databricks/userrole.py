from laktory.models.resources.databricks.userrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.userrole_base import UserRoleBase


class UserRole(UserRoleBase):
    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.role}-{self.user_id}"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
