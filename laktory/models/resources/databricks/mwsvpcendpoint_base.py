# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_vpc_endpoint
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsVpcEndpointGcpVpcEndpointInfo(BaseModel):
    endpoint_region: str = Field(..., description="Region of the PSC endpoint")
    project_id: str = Field(
        ...,
        description="The Google Cloud project ID of the VPC network where the PSC connection resides",
    )
    psc_connection_id: str | None = Field(
        None, description="The unique ID of this PSC connection"
    )
    psc_endpoint_name: str = Field(
        ..., description="The name of the PSC endpoint in the Google Cloud project"
    )
    service_attachment_id: str | None = Field(
        None, description="The service attachment this PSC connection connects to"
    )


class MwsVpcEndpointBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_vpc_endpoint`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    vpc_endpoint_name: str = Field(
        ..., description="Name of VPC Endpoint in Databricks Account"
    )
    account_id: str | None = Field(
        None,
        description="Account Id that could be found in the Accounts Console for [AWS](https://accounts.cloud.databricks.com/) or [GCP](https://accounts.gcp.databricks.com/)",
    )
    aws_account_id: str | None = Field(None)
    aws_endpoint_service_id: str | None = Field(
        None,
        description="(AWS Only) The ID of the Databricks endpoint service that this VPC endpoint is connected to. Please find the list of endpoint service IDs for each supported region in the [Databricks PrivateLink documentation](https://docs.databricks.com/administration-guide/cloud-configurations/aws/privatelink.html)",
    )
    aws_vpc_endpoint_id: str | None = Field(
        None,
        description="(AWS only) ID of configured [aws_vpc_endpoint](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/vpc_endpoint)",
    )
    region: str | None = Field(None, description="(AWS only) Region of AWS VPC")
    state: str | None = Field(None, description="(AWS Only) State of VPC Endpoint")
    use_case: str | None = Field(None)
    vpc_endpoint_id: str | None = Field(
        None,
        description="Canonical unique identifier of VPC Endpoint in Databricks Account",
    )
    gcp_vpc_endpoint_info: MwsVpcEndpointGcpVpcEndpointInfo | None = Field(
        None,
        description="(GCP only) a block consists of Google Cloud specific information for this PSC endpoint. It has the following fields:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_vpc_endpoint"


__all__ = ["MwsVpcEndpointBase", "MwsVpcEndpointGcpVpcEndpointInfo"]
