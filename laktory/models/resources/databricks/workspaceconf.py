from laktory.models.resources.databricks.workspaceconf_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspaceconf_base import WorkspaceConfBase


class WorkspaceConf(WorkspaceConfBase):
    """
    Databricks workspace configuration

    Expert workspace configuration properties (e.g. `enableIpAccessLists`,
    `maxTokenLifetimeDays`, `enableTokensConfig`). Declare a single instance
    per workspace: several instances setting the same key have no
    deterministic outcome.

    Examples
    --------
    ```py
    import io

    from laktory import models

    conf_yaml = '''
    custom_config:
      enableIpAccessLists: 'true'
      maxTokenLifetimeDays: '90'
    '''
    conf = models.resources.databricks.WorkspaceConf.model_validate_yaml(
        io.StringIO(conf_yaml)
    )
    ```

    References
    ----------

    * [Databricks workspace configuration](https://docs.databricks.com/api/workspace/workspaceconf)
    * [Terraform databricks_workspace_conf](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/workspace_conf)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
