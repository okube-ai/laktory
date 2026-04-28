from typing import Union

from pydantic import Field

from laktory.models.resources.databricks.metastoreassignment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.metastoreassignment_base import (
    MetastoreAssignmentBase,
)


class MetastoreAssignment(MetastoreAssignmentBase):
    """
    Databricks Metastore Assignment

    Examples
    --------
    ```py
    import io

    from laktory import models

    assignment_yaml = '''
    metastore_id: ${resources.metastore-prod.metastore_id}
    workspace_id: 1234567890
    default_catalog_name: dev
    '''
    assignment = models.resources.databricks.MetastoreAssignment.model_validate_yaml(
        io.StringIO(assignment_yaml)
    )
    ```

    References
    ----------

    * [Databricks Metastore Assignment](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/metastore_assignment)
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
