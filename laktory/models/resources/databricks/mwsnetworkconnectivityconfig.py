from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.mwsnccbinding import MwsNccBinding
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MwsNetworkConnectivityConfigEgressConfigDefaultRulesAwsStableIpRule(BaseModel):
    cidr_blocks: list[str] = Field(None, description="")


class MwsNetworkConnectivityConfigEgressConfigDefaultRulesAzureServiceEndpointRule(
    BaseModel
):
    subnets: list[str] = Field(None, description="")
    target_region: str = Field(None, description="")
    target_services: list[str] = Field(None, description="")


class MwsNetworkConnectivityConfigEgressConfigDefaultRules(BaseModel):
    aws_stable_ip_rule: MwsNetworkConnectivityConfigEgressConfigDefaultRulesAwsStableIpRule = Field(
        None, description=""
    )
    azure_service_endpoint_rule: MwsNetworkConnectivityConfigEgressConfigDefaultRulesAzureServiceEndpointRule = Field(
        None,
        description="""
    This provides a list of subnets. These subnets need to be allowed in your Azure resources in order for 
    Databricks to access.
    """,
    )


class MwsNetworkConnectivityConfigEgressConfigTargetRulesAzurePrivateEndpointRule(
    BaseModel
):
    connection_state: str = Field(None, description="")
    creation_time: int = Field(None, description="")
    deactivated: bool = Field(None, description="")
    deactivated_at: int = Field(None, description="")
    endpoint_name: str = Field(None, description="")
    group_id: str = Field(None, description="")
    network_connectivity_config_id: str = Field(
        None,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account",
    )
    resource_id: str = Field(None, description="")
    rule_id: str = Field(None, description="")
    updated_time: int = Field(None, description="")


class MwsNetworkConnectivityConfigEgressConfigTargetRules(BaseModel):
    azure_private_endpoint_rules: list[
        MwsNetworkConnectivityConfigEgressConfigTargetRulesAzurePrivateEndpointRule
    ] = Field(None, description="")


class MwsNetworkConnectivityConfigEgressConfig(BaseModel):
    default_rules: MwsNetworkConnectivityConfigEgressConfigDefaultRules = Field(
        None, description=""
    )
    target_rules: MwsNetworkConnectivityConfigEgressConfigTargetRules = Field(
        None, description=""
    )


class MwsNetworkConnectivityConfig(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Mws Network Connectivity Config

    Examples
    --------
    ```py
    ```
    """

    account_id: str = Field(None, description="")
    creation_time: int = Field(None, description="")
    egress_config: MwsNetworkConnectivityConfigEgressConfig = Field(
        None, description="Egress Config configration"
    )
    name: str = Field(
        ...,
        description="Name of Network Connectivity Config in Databricks Account. Change forces creation of a new resource.",
    )
    network_connectivity_config_id: str = Field(
        None,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account",
    )
    region: str = Field(
        ...,
        description="""
    Region of the Network Connectivity Config. NCCs can only be referenced by your workspaces in the same region.
    Change forces creation of a new resource.
    """,
    )
    updated_time: int = Field(None, description="")
    workspace_bindings: list[MwsNccBinding] = Field(None, description="")

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
