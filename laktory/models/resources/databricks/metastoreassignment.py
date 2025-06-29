from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MetastoreAssignment(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Metastore Assignment

    Examples
    --------
    ```py
    ```
    """

    default_catalog_name: str = Field(
        None, description="Unique identifier of the parent Metastore"
    )
    metastore_id: Union[int, str] = Field(
        None, description="id of the workspace for the assignment"
    )
    workspace_id: Union[int, str] = Field(
        None,
        description="Default catalog used for this assignment, default to hive_metastore",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self):
        return "assignment"

    @property
    def resource_key(self):
        return f"{self.metastore_id}-{self.workspace_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MetastoreAssignment"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_metastore_assignment"
