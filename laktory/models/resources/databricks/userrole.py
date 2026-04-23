from laktory.models.resources.databricks.userrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.userrole_base import UserRoleBase
from laktory.models.resources.pulumiresource import PulumiResource


class UserRole(UserRoleBase, PulumiResource):
    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.role}-{self.user_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:UserRole"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
