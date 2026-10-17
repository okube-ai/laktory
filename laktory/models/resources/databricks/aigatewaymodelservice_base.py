# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_ai_gateway_model_service
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AiGatewayModelServiceConfigInferenceTable(BaseModel):
    parent: str = Field(
        ...,
        description="Parent Unity Catalog schema where the inference table is created, in the form `schemas/{catalog}.{schema}`. Required when configuring an inference table. After the inference table is created, this field cannot be changed",
    )
    table_name_prefix: str | None = Field(
        None,
        description="Prefix used to form the inference table's registered name. AI Gateway appends `_payload`; for example, `table_name_prefix = 'orders'` creates `orders_payload`. If unset, the prefix defaults to the service name. Read `table` from the response for the resulting resource name. After the inference table is created, this field cannot be changed",
    )


class AiGatewayModelServiceConfigRateLimits(BaseModel):
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


class AiGatewayModelServiceConfigRoutingDestinationsExternalModelConfigTarget(
    BaseModel
):
    model: str = Field(
        ...,
        description="UC model FQN of the model served by the backing endpoint (e.g., `system.ai.databricks-claude-opus-4-6`). Resolved from Model Serving at Create/Update time",
    )
    native_api_types: list[str] | None = Field(
        None,
        description="Provider-native API types supported by this model, such as `openai/v1/chat/completions`. At least one value is required. AI Gateway uses these values to translate requests and responses. At most 64 entries of 256 characters each are allowed",
    )


class AiGatewayModelServiceConfigRoutingDestinationsExternalModelConfig(BaseModel):
    model_provider_service: str = Field(
        ...,
        description="Resource name of the governed ModelProviderService that owns provider auth and provider-specific configuration. The referenced ModelProviderService also carries the provider type, so this message does not surface it directly. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually",
    )
    target: (
        AiGatewayModelServiceConfigRoutingDestinationsExternalModelConfigTarget | None
    ) = Field(
        None,
        description="Routing target for the destination: the provider-side model selected from the referenced ModelProviderService's `targets` catalog, plus the unified API types the platform should translate to/from at request time",
    )


class AiGatewayModelServiceConfigRoutingDestinationsPayPerTokenConfig(BaseModel):
    model: str = Field(
        ...,
        description="UC model FQN of the model served by the backing endpoint (e.g., `system.ai.databricks-claude-opus-4-6`). Resolved from Model Serving at Create/Update time",
    )


class AiGatewayModelServiceConfigRoutingDestinationsProvisionedThroughputConfig(
    BaseModel
):
    model_serving_endpoint: str = Field(
        ...,
        description="Name of the backing Model Serving endpoint serving the provisioned- throughput foundation model, in the form `serving-endpoints/{name}`. The same Unity Catalog model can be served on multiple Model Serving endpoints with different throughput, regions, or configurations. The caller selects the endpoint to which this destination routes. The endpoint must exist at create time",
    )


class AiGatewayModelServiceConfigRoutingDestinations(BaseModel):
    destination_type: str = Field(
        ...,
        description="Backing-model category. Provide the matching type-specific configuration and leave the other type-specific configurations unset. Possible values are: `DESTINATION_TYPE_EXTERNAL_FOUNDATION_MODEL`, `DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL`, `DESTINATION_TYPE_PROVISIONED_THROUGHPUT_FOUNDATION_MODEL`",
    )
    external_model_config: (
        AiGatewayModelServiceConfigRoutingDestinationsExternalModelConfig | None
    ) = Field(
        None,
        description="Configuration for an external model reached through a model provider service",
    )
    name: str = Field(
        ...,
        description="Resource name of the model service. Format: `model-services/{catalog}.{schema}.{model_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_service_id`; required and immutable on Update/Get/Delete",
    )
    pay_per_token_config: (
        AiGatewayModelServiceConfigRoutingDestinationsPayPerTokenConfig | None
    ) = Field(
        None,
        description="Configuration for a pay-per-token Databricks foundation model",
    )
    provisioned_throughput_config: (
        AiGatewayModelServiceConfigRoutingDestinationsProvisionedThroughputConfig | None
    ) = Field(
        None,
        description="Configuration for a provisioned-throughput Databricks foundation model",
    )
    traffic_percentage: int | None = Field(
        None,
        description="Percentage of primary traffic sent to this destination, from 0 to 100. Required when there is more than one primary destination, in which case the primary percentages must sum to 100; a single primary destination receives all traffic. Fallback destinations are ordered and do not use this field",
    )


class AiGatewayModelServiceConfigRoutingFallbackDestinationsExternalModelConfigTarget(
    BaseModel
):
    model: str = Field(
        ...,
        description="UC model FQN of the model served by the backing endpoint (e.g., `system.ai.databricks-claude-opus-4-6`). Resolved from Model Serving at Create/Update time",
    )
    native_api_types: list[str] | None = Field(
        None,
        description="Provider-native API types supported by this model, such as `openai/v1/chat/completions`. At least one value is required. AI Gateway uses these values to translate requests and responses. At most 64 entries of 256 characters each are allowed",
    )


class AiGatewayModelServiceConfigRoutingFallbackDestinationsExternalModelConfig(
    BaseModel
):
    model_provider_service: str = Field(
        ...,
        description="Resource name of the governed ModelProviderService that owns provider auth and provider-specific configuration. The referenced ModelProviderService also carries the provider type, so this message does not surface it directly. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually",
    )
    target: (
        AiGatewayModelServiceConfigRoutingFallbackDestinationsExternalModelConfigTarget
        | None
    ) = Field(
        None,
        description="Routing target for the destination: the provider-side model selected from the referenced ModelProviderService's `targets` catalog, plus the unified API types the platform should translate to/from at request time",
    )


class AiGatewayModelServiceConfigRoutingFallbackDestinationsPayPerTokenConfig(
    BaseModel
):
    model: str = Field(
        ...,
        description="UC model FQN of the model served by the backing endpoint (e.g., `system.ai.databricks-claude-opus-4-6`). Resolved from Model Serving at Create/Update time",
    )


class AiGatewayModelServiceConfigRoutingFallbackDestinationsProvisionedThroughputConfig(
    BaseModel
):
    model_serving_endpoint: str = Field(
        ...,
        description="Name of the backing Model Serving endpoint serving the provisioned- throughput foundation model, in the form `serving-endpoints/{name}`. The same Unity Catalog model can be served on multiple Model Serving endpoints with different throughput, regions, or configurations. The caller selects the endpoint to which this destination routes. The endpoint must exist at create time",
    )


class AiGatewayModelServiceConfigRoutingFallbackDestinations(BaseModel):
    destination_type: str = Field(
        ...,
        description="Backing-model category. Provide the matching type-specific configuration and leave the other type-specific configurations unset. Possible values are: `DESTINATION_TYPE_EXTERNAL_FOUNDATION_MODEL`, `DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL`, `DESTINATION_TYPE_PROVISIONED_THROUGHPUT_FOUNDATION_MODEL`",
    )
    external_model_config: (
        AiGatewayModelServiceConfigRoutingFallbackDestinationsExternalModelConfig | None
    ) = Field(
        None,
        description="Configuration for an external model reached through a model provider service",
    )
    name: str = Field(
        ...,
        description="Resource name of the model service. Format: `model-services/{catalog}.{schema}.{model_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_service_id`; required and immutable on Update/Get/Delete",
    )
    pay_per_token_config: (
        AiGatewayModelServiceConfigRoutingFallbackDestinationsPayPerTokenConfig | None
    ) = Field(
        None,
        description="Configuration for a pay-per-token Databricks foundation model",
    )
    provisioned_throughput_config: (
        AiGatewayModelServiceConfigRoutingFallbackDestinationsProvisionedThroughputConfig
        | None
    ) = Field(
        None,
        description="Configuration for a provisioned-throughput Databricks foundation model",
    )
    traffic_percentage: int | None = Field(
        None,
        description="Percentage of primary traffic sent to this destination, from 0 to 100. Required when there is more than one primary destination, in which case the primary percentages must sum to 100; a single primary destination receives all traffic. Fallback destinations are ordered and do not use this field",
    )


class AiGatewayModelServiceConfigRoutingFallback(BaseModel):
    destinations: (
        list[AiGatewayModelServiceConfigRoutingFallbackDestinations] | None
    ) = Field(
        None,
        description="Primary routing destinations. At most 10 are allowed. At least one is required on Create. On Update, provide this list when replacing the full `config` or updating `config.routing.destinations`; other granular routing updates do not require resending destinations. The intermediate `config.routing` mask path is not supported",
    )


class AiGatewayModelServiceConfigRouting(BaseModel):
    destinations: list[AiGatewayModelServiceConfigRoutingDestinations] | None = Field(
        None,
        description="Primary routing destinations. At most 10 are allowed. At least one is required on Create. On Update, provide this list when replacing the full `config` or updating `config.routing.destinations`; other granular routing updates do not require resending destinations. The intermediate `config.routing` mask path is not supported",
    )
    fallback: AiGatewayModelServiceConfigRoutingFallback | None = Field(
        None,
        description="Fallback routing applied after a primary destination fails. Fallback destinations are tried in the listed order",
    )


class AiGatewayModelServiceConfig(BaseModel):
    inference_table: AiGatewayModelServiceConfigInferenceTable | None = Field(
        None, description="Inference table configuration for payload logging"
    )
    rate_limits: list[AiGatewayModelServiceConfigRateLimits] | None = Field(
        None,
        description="Rate limits applied to requests routed through this model service",
    )
    routing: AiGatewayModelServiceConfigRouting | None = Field(
        None, description="Routing configuration: destinations and fallback"
    )


class AiGatewayModelServiceProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AiGatewayModelServiceBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_ai_gateway_model_service`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    model_service_id: str = Field(
        ..., description="Name for the model service, e.g. 'my_model_service'"
    )
    parent: str = Field(
        ...,
        description="Parent Unity Catalog schema where the inference table is created, in the form `schemas/{catalog}.{schema}`. Required when configuring an inference table. After the inference table is created, this field cannot be changed",
    )
    comment: str | None = Field(None, description="User-provided description")
    config: AiGatewayModelServiceConfig | None = Field(
        None,
        description="Destinations, routing, rate limits, and payload logging configuration. Required on Create. On Update, provide this field when `update_mask` contains `config` or one of its subpaths",
    )
    provider_config: AiGatewayModelServiceProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_ai_gateway_model_service"


__all__ = [
    "AiGatewayModelServiceBase",
    "AiGatewayModelServiceConfig",
    "AiGatewayModelServiceConfigInferenceTable",
    "AiGatewayModelServiceConfigRateLimits",
    "AiGatewayModelServiceConfigRouting",
    "AiGatewayModelServiceConfigRoutingDestinations",
    "AiGatewayModelServiceConfigRoutingDestinationsExternalModelConfig",
    "AiGatewayModelServiceConfigRoutingDestinationsExternalModelConfigTarget",
    "AiGatewayModelServiceConfigRoutingDestinationsPayPerTokenConfig",
    "AiGatewayModelServiceConfigRoutingDestinationsProvisionedThroughputConfig",
    "AiGatewayModelServiceConfigRoutingFallback",
    "AiGatewayModelServiceConfigRoutingFallbackDestinations",
    "AiGatewayModelServiceConfigRoutingFallbackDestinationsExternalModelConfig",
    "AiGatewayModelServiceConfigRoutingFallbackDestinationsExternalModelConfigTarget",
    "AiGatewayModelServiceConfigRoutingFallbackDestinationsPayPerTokenConfig",
    "AiGatewayModelServiceConfigRoutingFallbackDestinationsProvisionedThroughputConfig",
    "AiGatewayModelServiceProviderConfig",
]
