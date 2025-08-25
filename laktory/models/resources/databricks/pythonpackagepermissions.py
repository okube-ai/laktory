from typing import Callable

from pydantic import Field
from pydantic import computed_field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

logger = get_logger(__name__)


class PythonPackagePermissions(BaseModel, PulumiResource, TerraformResource):
    access_controls: list[AccessControl] = Field(
        ..., description="Access controls list"
    )
    get_workspace_file_path: Callable = Field(
        ..., description="Callable returning workspace filepath", exclude=True
    )

    @computed_field(description="workspace_file_path")
    @property
    def workspace_file_path(self) -> str:
        return self.get_workspace_file_path()

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Permissions"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_permissions"
