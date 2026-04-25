from laktory.models.basemodel import PluralField
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.permissions_base import PermissionsBase


class Permissions(PermissionsBase):
    access_control: list[AccessControl] = PluralField(
        ...,
        description="Access controls list",
    )
