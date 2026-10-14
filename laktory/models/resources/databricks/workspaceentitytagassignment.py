from typing import Literal

from pydantic import Field

from laktory.models.resources.databricks.workspaceentitytagassignment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspaceentitytagassignment_base import (
    WorkspaceEntityTagAssignmentBase,
)


class WorkspaceEntityTagAssignment(WorkspaceEntityTagAssignmentBase):
    """
    Databricks Workspace Entity Tag Assignment

    Assigns a tag to a workspace entity (app, dashboard, Genie space or
    notebook). Tags can also be declared directly on `App`, `Dashboard` and
    `Notebook` with their `tags` field.

    Examples
    --------
    ```py
    import io

    from laktory import models

    tag_yaml = '''
    entity_type: apps
    entity_id: stock-prices-app
    tag_key: cost_center
    tag_value: finance
    '''
    tag = models.resources.databricks.WorkspaceEntityTagAssignment.model_validate_yaml(
        io.StringIO(tag_yaml)
    )
    ```

    References
    ----------

    * [Terraform databricks_workspace_entity_tag_assignment](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/workspace_entity_tag_assignment)
    """

    # Narrow the type from base's plain str
    entity_type: Literal["apps", "dashboards", "geniespaces", "notebooks"] = Field(
        ..., description="The type of entity to which the tag is assigned"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.entity_type}-{self.entity_id}-{self.tag_key}"
