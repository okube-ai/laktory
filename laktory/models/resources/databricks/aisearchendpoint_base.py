# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_ai_search_endpoint
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AiSearchEndpointCustomTags(BaseModel):
    key: str = Field(..., description="Key field for an AI Search endpoint tag")
    value: str | None = Field(
        None, description="[Optional] Value field for an AI Search endpoint tag"
    )


class AiSearchEndpointProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AiSearchEndpointBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_ai_search_endpoint`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    endpoint_type: str = Field(
        ...,
        description="Type of endpoint. Required on create and immutable thereafter. Possible values are: `STANDARD`, `STORAGE_OPTIMIZED`",
    )
    parent: str = Field(
        ...,
        description="The Workspace where this Endpoint will be created. Format: `workspaces/{workspace_id}`",
    )
    budget_policy_id: str | None = Field(
        None, description="The user-selected budget policy id for the endpoint"
    )
    endpoint_id: str | None = Field(
        None,
        description="The user-supplied short name for the Endpoint, per AIP-133. The server composes the full `Endpoint.name` as `{parent}/endpoints/{endpoint_id}`. AIP-133 does not list `endpoint_id` as a fields-may-be-required entry, so we annotate it OPTIONAL on the wire; the server still rejects empty values with INVALID_PARAMETER_VALUE",
    )
    replica_count: int | None = Field(
        None,
        description="The client-supplied desired number of replicas for the endpoint, applied at create/update time. Mutually exclusive with `target_qps`",
    )
    target_qps: int | None = Field(
        None,
        description="Target QPS for the endpoint. Mutually exclusive with `replica_count`. Best-effort; the system does not guarantee this QPS will be achieved",
    )
    usage_policy_id: str | None = Field(
        None, description="The usage policy id applied to the endpoint"
    )
    custom_tags: list[AiSearchEndpointCustomTags] | None = Field(
        None, description="The custom tags assigned to the endpoint"
    )
    provider_config: AiSearchEndpointProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_ai_search_endpoint"


__all__ = [
    "AiSearchEndpointBase",
    "AiSearchEndpointCustomTags",
    "AiSearchEndpointProviderConfig",
]
