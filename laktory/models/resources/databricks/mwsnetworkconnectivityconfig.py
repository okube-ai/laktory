from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

from laktory.models.resources.databricks.mwsnccbinding import MwsNccBinding


class MwsNetworkConnectivityConfigEgressConfigDefaultRulesAwsStableIpRule(BaseModel):
    """
    Attributes
    ----------
    cidr_blocks:
        todo
    """

    cidr_blocks: list[str] = None


class MwsNetworkConnectivityConfigEgressConfigDefaultRulesAzureServiceEndpointRule(
    BaseModel
):
    """
    Attributes
    ----------
    subnets:
        todo
    target_region:
        todo
    target_services:
        todo
    """

    subnets: list[str] = None
    target_region: str = None
    target_services: list[str] = None


class MwsNetworkConnectivityConfigEgressConfigDefaultRules(BaseModel):
    """
    Attributes
    ----------
    aws_stable_ip_rule:
        todo
    azure_service_endpoint_rule:
        This provides a list of subnets. These subnets need to be allowed in your Azure resources in order for
        Databricks to access.
    """

    aws_stable_ip_rule: (
        MwsNetworkConnectivityConfigEgressConfigDefaultRulesAwsStableIpRule
    ) = None
    azure_service_endpoint_rule: (
        MwsNetworkConnectivityConfigEgressConfigDefaultRulesAzureServiceEndpointRule
    ) = None


class MwsNetworkConnectivityConfigEgressConfigTargetRulesAzurePrivateEndpointRule(
    BaseModel
):
    """
    Attributes
    ----------
    connection_state:
        todo
    creation_time:
        todo
    deactivated:
        todo
    deactivated_at:
        todo
    endpoint_name:
        todo
    group_id:
        todo
    network_connectivity_config_id:
        Canonical unique identifier of Network Connectivity Config in Databricks Account
    resource_id:
        todo
    rule_id:
        todo
    updated_time:
        todo
    """

    connection_state: str = None
    creation_time: int = None
    deactivated: bool = None
    deactivated_at: int = None
    endpoint_name: str = None
    group_id: str = None
    network_connectivity_config_id: str = None
    resource_id: str = None
    rule_id: str = None
    updated_time: int = None


class MwsNetworkConnectivityConfigEgressConfigTargetRules(BaseModel):
    """
    Attributes
    ----------
    azure_private_endpoint_rules:
        todo
    """

    azure_private_endpoint_rules: list[
        MwsNetworkConnectivityConfigEgressConfigTargetRulesAzurePrivateEndpointRule
    ] = None


class MwsNetworkConnectivityConfigEgressConfig(BaseModel):
    """
    Databricks Metastore Data Access AWS IAM Role

    Attributes
    ----------
    default_rules:
        todo
    target_rules:
        todo
    """

    default_rules: MwsNetworkConnectivityConfigEgressConfigDefaultRules = None
    target_rules: MwsNetworkConnectivityConfigEgressConfigTargetRules = None


class MwsNetworkConnectivityConfig(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Mws Network Connectivity Config

    Attributes
    ----------
    account_id:
        todo
    creation_time:
        todo
    egress_config:
        Egress Config configration
    name:
        Name of Network Connectivity Config in Databricks Account. Change forces creation of a new resource.
    network_connectivity_config_id:
        Canonical unique identifier of Network Connectivity Config in Databricks Account
    region:
        Region of the Network Connectivity Config. NCCs can only be referenced by your workspaces in the same region.
        Change forces creation of a new resource.
    updated_time:
        todo

    Examples
    --------
    ```py
    ```
    """

    account_id: str = None
    creation_time: int = None
    egress_config: MwsNetworkConnectivityConfigEgressConfig = None
    name: str
    network_connectivity_config_id: str = None
    region: str
    updated_time: int = None
    workspace_bindings: list[MwsNccBinding] = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - workspace bindings
        """
        resources = []

        if self.workspace_bindings:
            for b in self.workspace_bindings:
                b.network_connectivity_config_id = f"${{resources.{self.resource_name}.network_connectivity_config_id}}"
                resources += [b]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MwsNetworkConnectivityConfig"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "workspace_bindings",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_network_connectivity_config"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
