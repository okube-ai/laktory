# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_networks
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsNetworksErrorMessages(BaseModel):
    error_message: str | None = Field(None)
    error_type: str | None = Field(None)


class MwsNetworksGcpNetworkInfo(BaseModel):
    network_project_id: str = Field(
        ..., description="The Google Cloud project ID of the VPC network"
    )
    pod_ip_range_name: str | None = Field(None)
    service_ip_range_name: str | None = Field(None)
    subnet_id: str = Field(
        ..., description="The ID of the subnet associated with this network"
    )
    subnet_region: str = Field(
        ...,
        description="The Google Cloud region of the workspace data plane. For example, `us-east4`",
    )
    vpc_id: str = Field(
        ...,
        description="The ID of the VPC associated with this network. VPC IDs can be used in multiple network configurations",
    )


class MwsNetworksVpcEndpoints(BaseModel):
    dataplane_relay: list[str] = Field(...)
    rest_api: list[str] = Field(...)


class MwsNetworksBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_networks`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str = Field(
        ...,
        description="Account Id that could be found in the top right corner of [Accounts Console](https://accounts.cloud.databricks.com/)",
    )
    network_name: str = Field(
        ..., description="name under which this network is registered"
    )
    creation_time: int | None = Field(None)
    network_id: str | None = Field(
        None,
        description="(String) id of network to be used for databricks_mws_workspaces resource",
    )
    security_group_ids: list[str] | None = Field(
        None,
        description="(AWS only) ids of [aws_security_group](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group)",
    )
    subnet_ids: list[str] | None = Field(
        None,
        description="(AWS only) ids of [aws_subnet](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/subnet)",
    )
    vpc_id: str | None = Field(
        None,
        description="The ID of the VPC associated with this network. VPC IDs can be used in multiple network configurations",
    )
    vpc_status: str | None = Field(None, description="(String) VPC attachment status")
    workspace_id: int | None = Field(
        None, description="(Integer) id of associated workspace"
    )
    error_messages: list[MwsNetworksErrorMessages] | None = Field(None)
    gcp_network_info: MwsNetworksGcpNetworkInfo | None = Field(
        None,
        description="(GCP only) a block consists of Google Cloud specific information for this network, for example the VPC ID, subnet ID, and secondary IP ranges. It has the following fields:",
    )
    vpc_endpoints: MwsNetworksVpcEndpoints | None = Field(
        None,
        description="mapping of databricks_mws_vpc_endpoint for PrivateLink or Private Service Connect connections",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_networks"


__all__ = [
    "MwsNetworksBase",
    "MwsNetworksErrorMessages",
    "MwsNetworksGcpNetworkInfo",
    "MwsNetworksVpcEndpoints",
]
