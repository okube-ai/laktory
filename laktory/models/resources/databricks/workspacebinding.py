from laktory.models.resources.databricks.workspacebinding_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspacebinding_base import (
    WorkspaceBindingBase,
)


class WorkspaceBinding(WorkspaceBindingBase):
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
