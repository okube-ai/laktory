from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MetastoreAssignment(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Metastore Assignment

    Attributes
    ----------
    metastore_id:
        Unique identifier of the parent Metastore
    workspace_id:
        id of the workspace for the assignment
    default_catalog_name:
        Default catalog used for this assignment, default to hive_metastore

    Examples
    --------
    ```py
    ```
    """

    default_catalog_name: str = None
    metastore_id: Union[int, str] = None
    workspace_id: Union[int, str] = None

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
