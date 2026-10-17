# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_ai_gateway_mcp_service
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AiGatewayMcpServiceConfigRateLimits(BaseModel):
    key: str = Field(
        ...,
        description="Scope of the rate limit. Depending on this value, the limit applies to a principal, the service as a whole, or each user by default. Possible values are: `RATE_LIMIT_KEY_SERVICE`, `RATE_LIMIT_KEY_SERVICE_PRINCIPAL`, `RATE_LIMIT_KEY_USER`, `RATE_LIMIT_KEY_USER_DEFAULT`, `RATE_LIMIT_KEY_USER_GROUP`",
    )
    principal: str | None = Field(
        None,
        description="Principal this limit applies to: user email, group name, or service principal application ID. Required when `key` applies to a user, group, or service principal; otherwise it must be unset",
    )
    renewal_period: str = Field(
        ...,
        description="Renewal period. Possible values are: `RATE_LIMIT_RENEWAL_PERIOD_HOUR`, `RATE_LIMIT_RENEWAL_PERIOD_MINUTE`",
    )
    requests: int | None = Field(
        None,
        description="Maximum requests allowed in one renewal period. Leave unset for no request limit. Set to `0` to deny all requests",
    )
    tokens: int | None = Field(
        None,
        description="Maximum tokens allowed in one renewal period. Leave unset for no token limit. Set to `0` to deny all requests",
    )


class AiGatewayMcpServiceConfigSourceConnection(BaseModel):
    name: str = Field(
        ...,
        description="Resource name of the MCP service. Format: `mcp-services/{catalog}.{schema}.{mcp_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `mcp_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayMcpServiceConfig(BaseModel):
    include_tool_selectors: list[str] | None = Field(
        None,
        description="Tool names or prefix patterns to expose from the MCP server. Use exact tool names or prefix patterns such as `read_*`. An empty list exposes all tools. At most 1,024 selectors are allowed, and each selector can contain at most 256 characters",
    )
    rate_limits: list[AiGatewayMcpServiceConfigRateLimits] | None = Field(
        None,
        description="Rate limits for tool invocations. Supported scopes are user, group, service principal, the service as a whole, and each user by default. Request and token limits are supported. Empty when no rate limit is configured",
    )
    source_connection: AiGatewayMcpServiceConfigSourceConnection | None = Field(
        None,
        description="Unity Catalog connection referencing the MCP server. Required on Create",
    )


class AiGatewayMcpServiceProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AiGatewayMcpServiceBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_ai_gateway_mcp_service`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    mcp_service_id: str = Field(
        ..., description="Name for the MCP service, e.g. 'my_mcp_service'"
    )
    parent: str = Field(
        ...,
        description="Name of the parent schema. Format: `schemas/{catalog}.{schema}`. Each `{...}` component is capped at 255 characters individually",
    )
    comment: str | None = Field(None, description="User-provided description")
    config: AiGatewayMcpServiceConfig | None = Field(
        None,
        description="Connection, tool selectors, and rate limits. Required on Create. On Update, provide this field when `update_mask` contains `config` or one of its subpaths",
    )
    provider_config: AiGatewayMcpServiceProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_ai_gateway_mcp_service"


__all__ = [
    "AiGatewayMcpServiceBase",
    "AiGatewayMcpServiceConfig",
    "AiGatewayMcpServiceConfigRateLimits",
    "AiGatewayMcpServiceConfigSourceConnection",
    "AiGatewayMcpServiceProviderConfig",
]
