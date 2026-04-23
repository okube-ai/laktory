from laktory.models.basemodel import PluralField
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.permissions_base import PermissionsBase
from laktory.models.resources.pulumiresource import PulumiResource


class Permissions(PermissionsBase, PulumiResource):
    access_control: list[AccessControl] = PluralField(
        ...,
        description="Access controls list",
    )

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Permissions"
