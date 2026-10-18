from pydantic import Field

from laktory.models.resources.databricks.accountnetworkpolicy_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.accountnetworkpolicy_base import (
    AccountNetworkPolicyBase,
)
from laktory.models.resources.databricks.workspacenetworkoption import (
    WorkspaceNetworkOption,
)


class AccountNetworkPolicy(AccountNetworkPolicyBase):
    """
    Databricks account network policy (account level)

    Network access rules (serverless egress destinations, ingress) that
    workspaces of the account can be bound to. Requires an account-level
    provider.

    Examples
    --------
    ```py
    import io

    from laktory import models

    policy_yaml = '''
    network_policy_id: restricted-egress
    egress:
      network_access:
        restriction_mode: RESTRICTED_ACCESS
        allowed_internet_destinations:
        - destination: pypi.org
          internet_destination_type: DNS_NAME
        policy_enforcement:
          enforcement_mode: ENFORCED
    workspace_ids:
    - 1234567890123456
    '''
    policy = models.resources.databricks.AccountNetworkPolicy.model_validate_yaml(
        io.StringIO(policy_yaml)
    )
    ```

    References
    ----------

    * [Databricks serverless egress control](https://docs.databricks.com/aws/en/security/network/serverless-network-security/network-policies)
    * [Terraform databricks_account_network_policy](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/account_network_policy)
    """

    # Laktory-specific
    workspace_ids: list[int] = Field(
        None,
        description="Workspaces the policy is applied to. Each is deployed as a `WorkspaceNetworkOption`.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.network_policy_id

    @property
    def additional_core_resources(self) -> list:
        """
        - workspace network options
        """
        return [
            WorkspaceNetworkOption(
                workspace_id=workspace_id,
                network_policy_id=f"${{resources.{self.resource_name}.network_policy_id}}",
            )
            for workspace_id in self.workspace_ids or []
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["workspace_ids"]
