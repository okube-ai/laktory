from typing import Union

from pydantic import Field

from laktory.models.resources.databricks.metastoreassignment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.metastoreassignment_base import (
    MetastoreAssignmentBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class MetastoreAssignment(MetastoreAssignmentBase, PulumiResource):
    """
    Databricks Metastore Assignment

    Examples
    --------
    ```py
    ```
    """

    # Laktory injects metastore_id from the parent Metastore resource
    metastore_id: Union[int, str, None] = Field(None, description="ID of the metastore")

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
