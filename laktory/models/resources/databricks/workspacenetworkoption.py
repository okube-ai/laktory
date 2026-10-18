from laktory.models.resources.databricks.workspacenetworkoption_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspacenetworkoption_base import (
    WorkspaceNetworkOptionBase,
)


class WorkspaceNetworkOption(WorkspaceNetworkOptionBase):
    """
    Databricks workspace network option (account level)

    Network policy applied to a workspace (serverless egress and ingress
    rules). Requires an account-level provider. Can also be declared in the
    `workspace_ids` of an `AccountNetworkPolicy`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    option_yaml = '''
    workspace_id: 1234567890123456
    network_policy_id: restricted-egress
    '''
    option = models.resources.databricks.WorkspaceNetworkOption.model_validate_yaml(
        io.StringIO(option_yaml)
    )
    ```

    References
    ----------

    * [Databricks serverless egress control](https://docs.databricks.com/aws/en/security/network/serverless-network-security/network-policies)
    * [Terraform databricks_workspace_network_option](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/workspace_network_option)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return str(self.workspace_id)
