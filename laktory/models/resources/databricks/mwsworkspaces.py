from laktory.models.resources.databricks.mwsworkspaces_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsworkspaces_base import MwsWorkspacesBase


class MwsWorkspaces(MwsWorkspacesBase):
    """
    Databricks Mws Workspaces

    Examples
    --------
    ```py
    import io

    from laktory import models

    workspace_yaml = '''
    account_id: ${vars.databricks_account_id}
    workspace_name: my-workspace
    aws_region: us-east-1
    credentials_id: ${resources.mws-credentials-this.credentials_id}
    storage_configuration_id: ${resources.mws-storage-configurations-this.storage_configuration_id}
    '''
    workspace = models.resources.databricks.MwsWorkspaces.model_validate_yaml(
        io.StringIO(workspace_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Workspaces](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_workspaces)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.workspace_name
