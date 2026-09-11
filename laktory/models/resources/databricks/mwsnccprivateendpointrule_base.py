# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_ncc_private_endpoint_rule
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsNccPrivateEndpointRuleGcpEndpoint(BaseModel):
    service_attachment: str | None = Field(None)


class MwsNccPrivateEndpointRuleBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_ncc_private_endpoint_rule`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    network_connectivity_config_id: str = Field(
        ...,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account. Change forces creation of a new resource",
    )
    domain_names: list[str] | None = Field(
        None,
        description="List of domain names of target private link service (Azure, conflicts with `group_id`) or target resource FQDNs accessible via the VPC endpoint service (AWS, conflicts with `resource_names`)",
    )
    enabled: bool | None = Field(
        None,
        description="Activation status. Only used by private endpoints towards an AWS S3 service",
    )
    endpoint_service: str | None = Field(
        None,
        description="(AWS only) Example `com.amazonaws.vpce.us-east-1.vpce-svc-123abcc1298abc123`. The full target AWS endpoint service name that connects to the destination resources of the private endpoint. Change forces creation of a new resource",
    )
    group_id: str | None = Field(
        None,
        description="(Azure only) Not used by customer-managed private endpoint services. The sub-resource type (group ID) of the target resource. Must be one of supported resource types (i.e., `blob`, `dfs`, `sqlServer` , etc. Consult the [Azure documentation](https://learn.microsoft.com/en-us/azure/private-link/private-endpoint-overview#private-link-resource) for full list of supported resources). Note that to connect to workspace root storage (root DBFS), you need two endpoints, one for `blob` and one for `dfs`. Change forces creation of a new resource. Conflicts with `domain_names`",
    )
    resource_id: str | None = Field(
        None,
        description="(Azure only) The Azure resource ID of the target resource. Change forces creation of a new resource",
    )
    resource_names: list[str] | None = Field(
        None,
        description="(AWS only) Only used by private endpoints towards AWS S3 service. List of globally unique S3 bucket names that will be accessed via the VPC endpoint. The bucket names must be in the same region as the NCC/endpoint service. Conflict with `domain_names`",
    )
    gcp_endpoint: MwsNccPrivateEndpointRuleGcpEndpoint | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_ncc_private_endpoint_rule"


__all__ = ["MwsNccPrivateEndpointRuleBase", "MwsNccPrivateEndpointRuleGcpEndpoint"]
