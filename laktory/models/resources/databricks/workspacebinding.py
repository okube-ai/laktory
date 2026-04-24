from laktory.models.resources.databricks.workspacebinding_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspacebinding_base import (
    WorkspaceBindingBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class WorkspaceBinding(WorkspaceBindingBase, PulumiResource):
    """
    Databricks Workspace Binding

    A binding of a workspace to some Databricks resource, such as catalog.


    Examples
    --------
    ```py
    ```
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self):
        return f"{self.securable_name}-{self.workspace_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:WorkspaceBinding"
