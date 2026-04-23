# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_network_connectivity_config
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class MwsNetworkConnectivityConfigEgressConfigDefaultRulesAwsStableIpRule(BaseModel):
    cidr_blocks: list[str] | None = Field(
        None,
        description="list of IP CIDR blocks. * `azure_service_endpoint_rule` (Azure only) - block with information about stable Azure service endpoints. You can configure the firewall of your Azure resources to allow traffic from your Databricks serverless compute resources.  Consists of the following fields:",
    )


class MwsNetworkConnectivityConfigEgressConfigDefaultRulesAzureServiceEndpointRule(
    BaseModel
):
    subnets: list[str] | None = Field(
        None,
        description="list of subnets from which Databricks network traffic originates when accessing your Azure resources",
    )
    target_region: str | None = Field(
        None, description="the Azure region in which this service endpoint rule applies"
    )
    target_services: list[str] | None = Field(
        None,
        description="the Azure services to which this service endpoint rule applies to",
    )


class MwsNetworkConnectivityConfigEgressConfigDefaultRules(BaseModel):
    aws_stable_ip_rule: (
        MwsNetworkConnectivityConfigEgressConfigDefaultRulesAwsStableIpRule | None
    ) = Field(None)
    azure_service_endpoint_rule: (
        MwsNetworkConnectivityConfigEgressConfigDefaultRulesAzureServiceEndpointRule
        | None
    ) = Field(None)


class MwsNetworkConnectivityConfigEgressConfigTargetRulesAwsPrivateEndpointRules(
    BaseModel
):
    account_id: str | None = Field(None)
    connection_state: str | None = Field(None)
    creation_time: float | None = Field(
        None, description="time in epoch milliseconds when this object was created"
    )
    deactivated: bool | None = Field(None)
    deactivated_at: float | None = Field(None)
    domain_names: list[str] | None = Field(None)
    enabled: bool | None = Field(None)
    endpoint_service: str | None = Field(None)
    error_message: str | None = Field(None)
    network_connectivity_config_id: str | None = Field(
        None,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account",
    )
    resource_names: list[str] | None = Field(None)
    rule_id: str | None = Field(None)
    updated_time: float | None = Field(
        None, description="time in epoch milliseconds when this object was updated"
    )
    vpc_endpoint_id: str | None = Field(None)


class MwsNetworkConnectivityConfigEgressConfigTargetRulesAzurePrivateEndpointRules(
    BaseModel
):
    connection_state: str | None = Field(None)
    creation_time: float | None = Field(
        None, description="time in epoch milliseconds when this object was created"
    )
    deactivated: bool | None = Field(None)
    deactivated_at: float | None = Field(None)
    domain_names: list[str] | None = Field(None)
    endpoint_name: str | None = Field(None)
    error_message: str | None = Field(None)
    group_id: str | None = Field(None)
    network_connectivity_config_id: str | None = Field(
        None,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account",
    )
    resource_id: str | None = Field(None)
    rule_id: str | None = Field(None)
    updated_time: float | None = Field(
        None, description="time in epoch milliseconds when this object was updated"
    )


class MwsNetworkConnectivityConfigEgressConfigTargetRules(BaseModel):
    aws_private_endpoint_rules: (
        list[MwsNetworkConnectivityConfigEgressConfigTargetRulesAwsPrivateEndpointRules]
        | None
    ) = PluralField(None, plural="aws_private_endpoint_ruless")
    azure_private_endpoint_rules: (
        list[
            MwsNetworkConnectivityConfigEgressConfigTargetRulesAzurePrivateEndpointRules
        ]
        | None
    ) = PluralField(None, plural="azure_private_endpoint_ruless")


class MwsNetworkConnectivityConfigEgressConfig(BaseModel):
    default_rules: MwsNetworkConnectivityConfigEgressConfigDefaultRules | None = Field(
        None,
        description="block describing network connectivity rules that are applied by default without resource specific configurations.  Consists of the following fields: * `aws_stable_ip_rule` (AWS only) - block with information about stable AWS IP CIDR blocks. You can use these to configure the firewall of your resources to allow traffic from your Databricks workspace.  Consists of the following fields:",
    )
    target_rules: MwsNetworkConnectivityConfigEgressConfigTargetRules | None = Field(
        None,
        description="block describing network connectivity rules that configured for each destinations. These rules override default rules.  Consists of the following fields: * `azure_private_endpoint_rules` (Azure only) - list containing information about configure Azure Private Endpoints. * `aws_private_endpoint_rules` (AWS only) - list containing information about configure AWS Private Endpoints",
    )


class MwsNetworkConnectivityConfigBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_network_connectivity_config`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ...,
        description="Name of the network connectivity configuration. The name can contain alphanumeric characters, hyphens, and underscores. The length must be between 3 and 30 characters. The name must match the regular expression `^[0-9a-zA-Z-_]{3,30}$`. Change forces creation of a new resource",
    )
    region: str = Field(
        ...,
        description="Region of the Network Connectivity Config. NCCs can only be referenced by your workspaces in the same region. Change forces creation of a new resource",
    )
    account_id: str | None = Field(None)
    creation_time: float | None = Field(
        None, description="time in epoch milliseconds when this object was created"
    )
    network_connectivity_config_id: str | None = Field(
        None,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account",
    )
    updated_time: float | None = Field(
        None, description="time in epoch milliseconds when this object was updated"
    )
    egress_config: MwsNetworkConnectivityConfigEgressConfig | None = Field(
        None,
        description="block containing information about network connectivity rules that apply to network traffic from your serverless compute resources. Consists of the following fields:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_network_connectivity_config"
