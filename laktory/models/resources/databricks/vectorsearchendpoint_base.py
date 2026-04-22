# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_vector_search_endpoint
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class VectorSearchEndpointScalingInfo(BaseModel):
    requested_min_qps: float | None = Field(None)
    state: str | None = Field(
        None,
        description="Current state of the endpoint. Currently following values are supported: `PROVISIONING`, `ONLINE`, and `OFFLINE`",
    )


class VectorSearchEndpointTimeouts(BaseModel):
    create: str | None = Field(None)


class VectorSearchEndpointBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_vector_search_endpoint`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    endpoint_type: str = Field(...)
    name: str = Field(
        ...,
        description="Name of the Mosaic AI Vector Search Endpoint to create. (Change leads to recreation of the resource). * `endpoint_type` (Required) Type of Mosaic AI Vector Search Endpoint.  Currently only accepting single value: `STANDARD` (See [documentation](https://docs.databricks.com/api/workspace/vectorsearchendpoints/createendpoint) for the list of currently supported values). (Change leads to recreation of the resource)",
    )
    budget_policy_id: str | None = Field(
        None, description="The Budget Policy ID set for this resource"
    )
    scaling_info: VectorSearchEndpointScalingInfo | None = Field(None)
    timeouts: VectorSearchEndpointTimeouts | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_vector_search_endpoint"
