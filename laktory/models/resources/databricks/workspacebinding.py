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
    import io

    from laktory import models

    binding_yaml = '''
    workspace_id: 1234567890
    securable_name: dev
    securable_type: catalog
    binding_type: BINDING_TYPE_READ_WRITE
    '''
    binding = models.resources.databricks.WorkspaceBinding.model_validate_yaml(
        io.StringIO(binding_yaml)
    )
    ```

    References
    ----------

    * [Databricks Workspace Binding](https://docs.databricks.com/en/data-governance/unity-catalog/binding.html)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self):
        return f"{self.securable_name}-{self.workspace_id}"
