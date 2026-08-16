# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_model_serving
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ModelServingAiGatewayFallbackConfig(BaseModel):
    enabled: bool = Field(
        ..., description="boolean flag specifying if usage tracking is enabled"
    )


class ModelServingAiGatewayGuardrailsInputPii(BaseModel):
    behavior: str | None = Field(
        None,
        description="a string that describes the behavior for PII filter. Currently only `BLOCK` value is supported",
    )


class ModelServingAiGatewayGuardrailsInput(BaseModel):
    invalid_keywords: list[str] | None = Field(
        None,
        description="(Deprecated) List of invalid keywords. AI guardrail uses keyword or string matching to decide if the keyword exists in the request or response content",
    )
    safety: bool | None = Field(
        None,
        description="the boolean flag that indicates whether the safety filter is enabled",
    )
    valid_topics: list[str] | None = Field(
        None,
        description="(Deprecated) The list of allowed topics. Given a chat request, this guardrail flags the request if its topic is not in the allowed topics",
    )
    pii: ModelServingAiGatewayGuardrailsInputPii | None = Field(
        None, description="Block with configuration for guardrail PII filter:"
    )


class ModelServingAiGatewayGuardrailsOutputPii(BaseModel):
    behavior: str | None = Field(
        None,
        description="a string that describes the behavior for PII filter. Currently only `BLOCK` value is supported",
    )


class ModelServingAiGatewayGuardrailsOutput(BaseModel):
    invalid_keywords: list[str] | None = Field(
        None,
        description="(Deprecated) List of invalid keywords. AI guardrail uses keyword or string matching to decide if the keyword exists in the request or response content",
    )
    safety: bool | None = Field(
        None,
        description="the boolean flag that indicates whether the safety filter is enabled",
    )
    valid_topics: list[str] | None = Field(
        None,
        description="(Deprecated) The list of allowed topics. Given a chat request, this guardrail flags the request if its topic is not in the allowed topics",
    )
    pii: ModelServingAiGatewayGuardrailsOutputPii | None = Field(
        None, description="Block with configuration for guardrail PII filter:"
    )


class ModelServingAiGatewayGuardrails(BaseModel):
    input: ModelServingAiGatewayGuardrailsInput | None = Field(
        None, description="A block with configuration for input guardrail filters:"
    )
    output: ModelServingAiGatewayGuardrailsOutput | None = Field(
        None,
        description="A block with configuration for output guardrail filters.  Has the same structure as `input` block",
    )


class ModelServingAiGatewayInferenceTableConfig(BaseModel):
    catalog_name: str | None = Field(
        None,
        description="The name of the catalog in Unity Catalog. NOTE: On update, you cannot change the catalog name if it was already set",
    )
    enabled: bool | None = Field(
        None, description="boolean flag specifying if usage tracking is enabled"
    )
    schema_name: str | None = Field(
        None,
        description="The name of the schema in Unity Catalog. NOTE: On update, you cannot change the schema name if it was already set",
    )
    table_name_prefix: str | None = Field(
        None,
        description="The prefix of the table in Unity Catalog. NOTE: On update, you cannot change the prefix name if it was already set",
    )


class ModelServingAiGatewayRateLimits(BaseModel):
    calls: int | None = Field(
        None,
        description="Used to specify how many calls are allowed for a key within the renewal_period",
    )
    key: str | None = Field(
        None,
        description="Key field for a serving endpoint rate limit. Currently, `user`, `user_group`, `service_principal`, and `endpoint` are supported, with `endpoint` being the default if not specified",
    )
    principal: str | None = Field(
        None,
        description="Principal field for a user, user group, or service principal to apply rate limiting to. Accepts a user email, group name, or service principal application ID",
    )
    renewal_period: str = Field(
        ...,
        description="Renewal period field for a serving endpoint rate limit. Currently, only `minute` is supported",
    )
    tokens: int | None = Field(
        None,
        description="Specifies how many tokens are allowed for a key within the renewal_period",
    )


class ModelServingAiGatewayUsageTrackingConfig(BaseModel):
    enabled: bool | None = Field(
        None, description="boolean flag specifying if usage tracking is enabled"
    )


class ModelServingAiGateway(BaseModel):
    fallback_config: ModelServingAiGatewayFallbackConfig | None = Field(
        None,
        description="block with configuration for traffic fallback which auto fallbacks to other served entities if the request to a served entity fails with certain error codes, to increase availability",
    )
    guardrails: ModelServingAiGatewayGuardrails | None = Field(
        None,
        description="Block with configuration for AI Guardrails to prevent unwanted data and unsafe data in requests and responses. Consists of the following attributes:",
    )
    inference_table_config: ModelServingAiGatewayInferenceTableConfig | None = Field(
        None,
        description="Block describing the configuration of usage tracking. Consists of the following attributes:",
    )
    rate_limits: list[ModelServingAiGatewayRateLimits] | None = Field(
        None,
        description="Block describing rate limits for AI gateway. For details see the description of `rate_limits` block above",
    )
    usage_tracking_config: ModelServingAiGatewayUsageTrackingConfig | None = Field(
        None,
        description="Block with configuration for payload logging using inference tables. For details see the description of `auto_capture_config` block above",
    )


class ModelServingConfigAutoCaptureConfig(BaseModel):
    catalog_name: str | None = Field(
        None,
        description="The name of the catalog in Unity Catalog. NOTE: On update, you cannot change the catalog name if it was already set",
    )
    enabled: bool | None = Field(
        None, description="boolean flag specifying if usage tracking is enabled"
    )
    schema_name: str | None = Field(
        None,
        description="The name of the schema in Unity Catalog. NOTE: On update, you cannot change the schema name if it was already set",
    )
    table_name_prefix: str | None = Field(
        None,
        description="The prefix of the table in Unity Catalog. NOTE: On update, you cannot change the prefix name if it was already set",
    )


class ModelServingConfigServedEntitiesExternalModelAi21labsConfig(BaseModel):
    ai21labs_api_key: str | None = Field(
        None, description="The Databricks secret key reference for an AI21Labs API key"
    )
    ai21labs_api_key_plaintext: str | None = Field(
        None, description="An AI21 Labs API key provided as a plaintext string"
    )


class ModelServingConfigServedEntitiesExternalModelAmazonBedrockConfig(BaseModel):
    aws_access_key_id: str | None = Field(
        None,
        description="The Databricks secret key reference for an AWS Access Key ID with permissions to interact with Bedrock services",
    )
    aws_access_key_id_plaintext: str | None = Field(
        None,
        description="An AWS access key ID with permissions to interact with Bedrock services provided as a plaintext string",
    )
    aws_region: str = Field(
        ..., description="The AWS region to use. Bedrock has to be enabled there"
    )
    aws_secret_access_key: str | None = Field(
        None,
        description="The Databricks secret key reference for an AWS Secret Access Key paired with the access key ID, with permissions to interact with Bedrock services",
    )
    aws_secret_access_key_plaintext: str | None = Field(
        None,
        description="An AWS secret access key paired with the access key ID, with permissions to interact with Bedrock services provided as a plaintext string",
    )
    bedrock_provider: str = Field(
        ...,
        description="The underlying provider in Amazon Bedrock. Supported values (case insensitive) include: `Anthropic`, `Cohere`, `AI21Labs`, `Amazon`",
    )
    instance_profile_arn: str | None = Field(
        None,
        description="ARN of the instance profile that the served model will use to access AWS resources",
    )


class ModelServingConfigServedEntitiesExternalModelAnthropicConfig(BaseModel):
    anthropic_api_key: str | None = Field(
        None, description="The Databricks secret key reference for an Anthropic API key"
    )
    anthropic_api_key_plaintext: str | None = Field(
        None, description="The Anthropic API key provided as a plaintext string"
    )


class ModelServingConfigServedEntitiesExternalModelCohereConfig(BaseModel):
    cohere_api_base: str | None = Field(None)
    cohere_api_key: str | None = Field(
        None, description="The Databricks secret key reference for a Cohere API key"
    )
    cohere_api_key_plaintext: str | None = Field(
        None, description="The Cohere API key provided as a plaintext string"
    )


class ModelServingConfigServedEntitiesExternalModelCustomProviderConfigApiKeyAuth(
    BaseModel
):
    key: str = Field(
        ...,
        description="Key field for a serving endpoint rate limit. Currently, `user`, `user_group`, `service_principal`, and `endpoint` are supported, with `endpoint` being the default if not specified",
    )
    value: str | None = Field(None, description="The value field for a tag")
    value_plaintext: str | None = Field(
        None, description="The API Key provided as a plaintext string"
    )


class ModelServingConfigServedEntitiesExternalModelCustomProviderConfigBearerTokenAuth(
    BaseModel
):
    token: str | None = Field(
        None, description="The Databricks secret key reference for a token"
    )
    token_plaintext: str | None = Field(
        None, description="The token provided as a plaintext string"
    )


class ModelServingConfigServedEntitiesExternalModelCustomProviderConfig(BaseModel):
    custom_provider_url: str = Field(..., description="URL of the custom provider API")
    api_key_auth: (
        ModelServingConfigServedEntitiesExternalModelCustomProviderConfigApiKeyAuth
        | None
    ) = Field(
        None,
        description="API key authentication for the custom provider API. Conflicts with `bearer_token_auth`",
    )
    bearer_token_auth: (
        ModelServingConfigServedEntitiesExternalModelCustomProviderConfigBearerTokenAuth
        | None
    ) = Field(
        None,
        description="bearer token authentication for the custom provider API.  Conflicts with `api_key_auth`",
    )


class ModelServingConfigServedEntitiesExternalModelDatabricksModelServingConfig(
    BaseModel
):
    databricks_api_token: str | None = Field(
        None,
        description="The Databricks secret key reference for a Databricks API token that corresponds to a user or service principal with Can Query access to the model serving endpoint pointed to by this external model",
    )
    databricks_api_token_plaintext: str | None = Field(
        None,
        description="The Databricks API token that corresponds to a user or service principal with Can Query access to the model serving endpoint pointed to by this external model provided as a plaintext string",
    )
    databricks_workspace_url: str = Field(
        ...,
        description="The URL of the Databricks workspace containing the model serving endpoint pointed to by this external model",
    )


class ModelServingConfigServedEntitiesExternalModelGoogleCloudVertexAiConfig(BaseModel):
    private_key: str | None = Field(
        None,
        description="The Databricks secret key reference for a private key for the service account that has access to the Google Cloud Vertex AI Service",
    )
    private_key_plaintext: str | None = Field(
        None,
        description="The private key for the service account that has access to the Google Cloud Vertex AI Service is provided as a plaintext secret",
    )
    project_id: str = Field(
        ...,
        description="This is the Google Cloud project id that the service account is associated with",
    )
    region: str = Field(
        ..., description="This is the region for the Google Cloud Vertex AI Service"
    )


class ModelServingConfigServedEntitiesExternalModelOpenaiConfig(BaseModel):
    microsoft_entra_client_id: str | None = Field(
        None,
        description="This field is only required for Azure AD OpenAI and is the Microsoft Entra Client ID",
    )
    microsoft_entra_client_secret: str | None = Field(
        None,
        description="The Databricks secret key reference for a client secret used for Microsoft Entra ID authentication",
    )
    microsoft_entra_client_secret_plaintext: str | None = Field(
        None,
        description="The client secret used for Microsoft Entra ID authentication provided as a plaintext string",
    )
    microsoft_entra_tenant_id: str | None = Field(
        None,
        description="This field is only required for Azure AD OpenAI and is the Microsoft Entra Tenant ID",
    )
    openai_api_base: str | None = Field(
        None,
        description="This is the base URL for the OpenAI API (default: '<https://api.openai.com/v1>'). For Azure OpenAI, this field is required and is the base URL for the Azure OpenAI API service provided by Azure",
    )
    openai_api_key: str | None = Field(
        None,
        description="The Databricks secret key reference for an OpenAI or Azure OpenAI API key",
    )
    openai_api_key_plaintext: str | None = Field(
        None,
        description="The OpenAI API key using the OpenAI or Azure service provided as a plaintext string",
    )
    openai_api_type: str | None = Field(
        None,
        description="This is an optional field to specify the type of OpenAI API to use. For Azure OpenAI, this field is required, and this parameter represents the preferred security access validation protocol. For access token validation, use `azure`. For authentication using Azure Active Directory (Azure AD) use, `azuread`",
    )
    openai_api_version: str | None = Field(
        None,
        description="This is an optional field to specify the OpenAI API version. For Azure OpenAI, this field is required and is the version of the Azure OpenAI service to utilize, specified by a date",
    )
    openai_deployment_name: str | None = Field(
        None,
        description="This field is only required for Azure OpenAI and is the name of the deployment resource for the Azure OpenAI service",
    )
    openai_organization: str | None = Field(
        None,
        description="This is an optional field to specify the organization in OpenAI or Azure OpenAI",
    )


class ModelServingConfigServedEntitiesExternalModelPalmConfig(BaseModel):
    palm_api_key: str | None = Field(
        None, description="The Databricks secret key reference for a PaLM API key"
    )
    palm_api_key_plaintext: str | None = Field(
        None, description="The PaLM API key provided as a plaintext string"
    )


class ModelServingConfigServedEntitiesExternalModel(BaseModel):
    name: str = Field(
        ...,
        description="The name of a served model. It must be unique across an endpoint. If not specified, this field will default to `modelname-modelversion`. A served model name can consist of alphanumeric characters, dashes, and underscores",
    )
    provider: str = Field(
        ...,
        description="The name of the provider for the external model. Currently, the supported providers are `ai21labs`, `anthropic`, `amazon-bedrock`, `cohere`, `databricks-model-serving`, `google-cloud-vertex-ai`, `openai`, and `palm`",
    )
    task: str = Field(..., description="The task type of the external model")
    ai21labs_config: (
        ModelServingConfigServedEntitiesExternalModelAi21labsConfig | None
    ) = Field(None, description="AI21Labs Config")
    amazon_bedrock_config: (
        ModelServingConfigServedEntitiesExternalModelAmazonBedrockConfig | None
    ) = Field(None, description="Amazon Bedrock Config")
    anthropic_config: (
        ModelServingConfigServedEntitiesExternalModelAnthropicConfig | None
    ) = Field(None, description="Anthropic Config")
    cohere_config: ModelServingConfigServedEntitiesExternalModelCohereConfig | None = (
        Field(None, description="Cohere Config")
    )
    custom_provider_config: (
        ModelServingConfigServedEntitiesExternalModelCustomProviderConfig | None
    ) = Field(
        None,
        description="Custom Provider Config. Only required if the provider is 'custom'",
    )
    databricks_model_serving_config: (
        ModelServingConfigServedEntitiesExternalModelDatabricksModelServingConfig | None
    ) = Field(None, description="Databricks Model Serving Config")
    google_cloud_vertex_ai_config: (
        ModelServingConfigServedEntitiesExternalModelGoogleCloudVertexAiConfig | None
    ) = Field(None, description="Google Cloud Vertex AI Config")
    openai_config: ModelServingConfigServedEntitiesExternalModelOpenaiConfig | None = (
        Field(None, description="OpenAI Config")
    )
    palm_config: ModelServingConfigServedEntitiesExternalModelPalmConfig | None = Field(
        None, description="PaLM Config"
    )


class ModelServingConfigServedEntities(BaseModel):
    burst_scaling_enabled: bool | None = Field(None)
    entity_name: str | None = Field(
        None,
        description="The name of the entity to be served. The entity may be a model in the Databricks Model Registry, a model in the Unity Catalog (UC), or a function of type `FEATURE_SPEC` in the UC. If it is a UC object, the full name of the object should be given in the form of `catalog_name.schema_name.model_name`",
    )
    entity_version: str | None = Field(
        None,
        description="The version of the model in Databricks Model Registry to be served or empty if the entity is a `FEATURE_SPEC`",
    )
    environment_vars: dict[str, str] | None = Field(
        None,
        description="a map of environment variable names/values that will be used for serving this model.  Environment variables may refer to Databricks secrets using the standard syntax: `{{secrets/secret_scope/secret_key}}`",
    )
    instance_profile_arn: str | None = Field(
        None,
        description="ARN of the instance profile that the served model will use to access AWS resources",
    )
    max_provisioned_concurrency: int | None = Field(
        None,
        description="The maximum provisioned concurrency that the endpoint can scale up to. Conflicts with `workload_size`",
    )
    max_provisioned_throughput: int | None = Field(
        None,
        description="The maximum tokens per second that the endpoint can scale up to",
    )
    min_provisioned_concurrency: int | None = Field(
        None,
        description="The minimum provisioned concurrency that the endpoint can scale down to. Conflicts with `workload_size`",
    )
    min_provisioned_throughput: int | None = Field(
        None,
        description="The minimum tokens per second that the endpoint can scale down to",
    )
    name: str | None = Field(
        None,
        description="The name of a served model. It must be unique across an endpoint. If not specified, this field will default to `modelname-modelversion`. A served model name can consist of alphanumeric characters, dashes, and underscores",
    )
    provisioned_model_units: int | None = Field(None)
    scale_to_zero_enabled: bool | None = Field(
        None,
        description="Whether the compute resources for the served model should scale down to zero. If `scale-to-zero` is enabled, the lower bound of the provisioned concurrency for each workload size will be 0. The default value is `true`",
    )
    workload_size: str | None = Field(
        None,
        description="The workload size of the served model. The workload size corresponds to a range of provisioned concurrency that the compute will autoscale between. A single unit of provisioned concurrency can process one request at a time. Valid workload sizes are `Small` (4 - 4 provisioned concurrency), `Medium` (8 - 16 provisioned concurrency), and `Large` (16 - 64 provisioned concurrency)",
    )
    workload_type: str | None = Field(
        None,
        description="The workload type of the served model. The workload type selects which type of compute to use in the endpoint. For deep learning workloads, GPU acceleration is available by selecting workload types like `GPU_SMALL` and others. See the documentation for all options. The default value is `CPU`",
    )
    external_model: ModelServingConfigServedEntitiesExternalModel | None = Field(
        None,
        description="The external model to be served. NOTE: Only one of `external_model` and (`entity_name`, `entity_version`, `workload_size`, `workload_type`, and `scale_to_zero_enabled`) can be specified with the latter set being used for custom model serving for a Databricks registered model. When an `external_model` is present, the served entities list can only have one `served_entity` object. An existing endpoint with `external_model` can not be updated to an endpoint without `external_model`. If the endpoint is created without `external_model`, users cannot update it to add `external_model` later",
    )


class ModelServingConfigServedModels(BaseModel):
    burst_scaling_enabled: bool | None = Field(None)
    environment_vars: dict[str, str] | None = Field(
        None,
        description="a map of environment variable names/values that will be used for serving this model.  Environment variables may refer to Databricks secrets using the standard syntax: `{{secrets/secret_scope/secret_key}}`",
    )
    instance_profile_arn: str | None = Field(
        None,
        description="ARN of the instance profile that the served model will use to access AWS resources",
    )
    max_provisioned_concurrency: int | None = Field(
        None,
        description="The maximum provisioned concurrency that the endpoint can scale up to. Conflicts with `workload_size`",
    )
    max_provisioned_throughput: int | None = Field(
        None,
        description="The maximum tokens per second that the endpoint can scale up to",
    )
    min_provisioned_concurrency: int | None = Field(
        None,
        description="The minimum provisioned concurrency that the endpoint can scale down to. Conflicts with `workload_size`",
    )
    min_provisioned_throughput: int | None = Field(
        None,
        description="The minimum tokens per second that the endpoint can scale down to",
    )
    model_name: str = Field(
        ...,
        description="The name of the model in Databricks Model Registry to be served",
    )
    model_version: str = Field(
        ...,
        description="The version of the model in Databricks Model Registry to be served",
    )
    name: str | None = Field(
        None,
        description="The name of a served model. It must be unique across an endpoint. If not specified, this field will default to `modelname-modelversion`. A served model name can consist of alphanumeric characters, dashes, and underscores",
    )
    provisioned_model_units: int | None = Field(None)
    scale_to_zero_enabled: bool | None = Field(
        None,
        description="Whether the compute resources for the served model should scale down to zero. If `scale-to-zero` is enabled, the lower bound of the provisioned concurrency for each workload size will be 0. The default value is `true`",
    )
    workload_size: str | None = Field(
        None,
        description="The workload size of the served model. The workload size corresponds to a range of provisioned concurrency that the compute will autoscale between. A single unit of provisioned concurrency can process one request at a time. Valid workload sizes are `Small` (4 - 4 provisioned concurrency), `Medium` (8 - 16 provisioned concurrency), and `Large` (16 - 64 provisioned concurrency)",
    )
    workload_type: str | None = Field(
        None,
        description="The workload type of the served model. The workload type selects which type of compute to use in the endpoint. For deep learning workloads, GPU acceleration is available by selecting workload types like `GPU_SMALL` and others. See the documentation for all options. The default value is `CPU`",
    )


class ModelServingConfigTrafficConfigRoutes(BaseModel):
    served_entity_name: str | None = Field(
        None,
        description="The name of the served entity this route configures traffic for. This needs to match the name of a `served_entity` block",
    )
    served_model_name: str | None = Field(None)
    traffic_percentage: int = Field(
        ...,
        description="The percentage of endpoint traffic to send to this route. It must be an integer between 0 and 100 inclusive",
    )


class ModelServingConfigTrafficConfig(BaseModel):
    routes: list[ModelServingConfigTrafficConfigRoutes] | None = Field(
        None,
        description="Each block represents a route that defines traffic to each served entity. Each `served_entity` block needs to have a corresponding `routes` block",
    )


class ModelServingConfig(BaseModel):
    auto_capture_config: ModelServingConfigAutoCaptureConfig | None = Field(
        None,
        description="Configuration for Inference Tables which automatically logs requests and responses to Unity Catalog",
    )
    served_entities: list[ModelServingConfigServedEntities] | None = Field(
        None,
        description="A list of served entities for the endpoint to serve. A serving endpoint can have up to 10 served entities",
    )
    served_models: list[ModelServingConfigServedModels] | None = Field(
        None,
        description="(Deprecated, use `served_entities` instead) Each block represents a served model for the endpoint to serve. A model serving endpoint can have up to 10 served models",
    )
    traffic_config: ModelServingConfigTrafficConfig | None = Field(
        None,
        description="A single block represents the traffic split configuration amongst the served models",
    )


class ModelServingEmailNotifications(BaseModel):
    on_update_failure: list[str] | None = Field(
        None,
        description="a list of email addresses to be notified when an endpoint fails to update its configuration or state",
    )
    on_update_success: list[str] | None = Field(
        None,
        description="a list of email addresses to be notified when an endpoint successfully updates its configuration or state",
    )


class ModelServingRateLimits(BaseModel):
    calls: int = Field(
        ...,
        description="Used to specify how many calls are allowed for a key within the renewal_period",
    )
    key: str | None = Field(
        None,
        description="Key field for a serving endpoint rate limit. Currently, `user`, `user_group`, `service_principal`, and `endpoint` are supported, with `endpoint` being the default if not specified",
    )
    renewal_period: str = Field(
        ...,
        description="Renewal period field for a serving endpoint rate limit. Currently, only `minute` is supported",
    )


class ModelServingTags(BaseModel):
    key: str = Field(
        ...,
        description="Key field for a serving endpoint rate limit. Currently, `user`, `user_group`, `service_principal`, and `endpoint` are supported, with `endpoint` being the default if not specified",
    )
    value: str | None = Field(None, description="The value field for a tag")


class ModelServingTelemetryConfigInferenceTableConfig(BaseModel):
    name: str | None = Field(
        None,
        description="The name of a served model. It must be unique across an endpoint. If not specified, this field will default to `modelname-modelversion`. A served model name can consist of alphanumeric characters, dashes, and underscores",
    )
    sampling_fraction: int | None = Field(None)


class ModelServingTelemetryConfigTableNames(BaseModel):
    annotations_table: str | None = Field(None)
    logs_table: str | None = Field(None)
    metrics_table: str | None = Field(None)
    traces_table: str | None = Field(None)


class ModelServingTelemetryConfig(BaseModel):
    telemetry_profile_id: str | None = Field(None)
    inference_table_config: ModelServingTelemetryConfigInferenceTableConfig | None = (
        Field(
            None,
            description="Block describing the configuration of usage tracking. Consists of the following attributes:",
        )
    )
    table_names: ModelServingTelemetryConfigTableNames | None = Field(None)


class ModelServingTimeouts(BaseModel):
    create: str | None = Field(None)
    update_: str | None = Field(
        None,
        serialization_alias="update",
        validation_alias=AliasChoices("update", "update_"),
    )


class ModelServingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_model_serving`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ...,
        description="The name of a served model. It must be unique across an endpoint. If not specified, this field will default to `modelname-modelversion`. A served model name can consist of alphanumeric characters, dashes, and underscores",
    )
    budget_policy_id: str | None = Field(
        None, description="(Optiona) The Budget Policy ID set for this serving endpoint"
    )
    description: str | None = Field(
        None, description="The description of the model serving endpoint"
    )
    route_optimized: bool | None = Field(
        None,
        description="A boolean enabling route optimization for the endpoint. *Note: only available for custom models.*",
    )
    ai_gateway: ModelServingAiGateway | None = Field(
        None,
        description="A block with AI Gateway configuration for the serving endpoint. *Note: only external model endpoints are supported as of now.*",
    )
    config: ModelServingConfig | None = Field(
        None,
        description="The config for the external model, which must match the provider. *Note that API keys could be provided either as a reference to the Databricks Secret (parameters without `_plaintext` suffix) or in plain text (parameters with `_plaintext` suffix)!*",
    )
    email_notifications: ModelServingEmailNotifications | None = Field(
        None, description="A block with Email notification setting"
    )
    rate_limits: list[ModelServingRateLimits] | None = Field(
        None,
        description="Block describing rate limits for AI gateway. For details see the description of `rate_limits` block above",
    )
    tags: list[ModelServingTags] | None = Field(
        None,
        description="Tags to be attached to the serving endpoint and automatically propagated to billing logs",
    )
    telemetry_config: ModelServingTelemetryConfig | None = Field(None)
    timeouts: ModelServingTimeouts | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_model_serving"


__all__ = [
    "ModelServingAiGateway",
    "ModelServingAiGatewayFallbackConfig",
    "ModelServingAiGatewayGuardrails",
    "ModelServingAiGatewayGuardrailsInput",
    "ModelServingAiGatewayGuardrailsInputPii",
    "ModelServingAiGatewayGuardrailsOutput",
    "ModelServingAiGatewayGuardrailsOutputPii",
    "ModelServingAiGatewayInferenceTableConfig",
    "ModelServingAiGatewayRateLimits",
    "ModelServingAiGatewayUsageTrackingConfig",
    "ModelServingBase",
    "ModelServingConfig",
    "ModelServingConfigAutoCaptureConfig",
    "ModelServingConfigServedEntities",
    "ModelServingConfigServedEntitiesExternalModel",
    "ModelServingConfigServedEntitiesExternalModelAi21labsConfig",
    "ModelServingConfigServedEntitiesExternalModelAmazonBedrockConfig",
    "ModelServingConfigServedEntitiesExternalModelAnthropicConfig",
    "ModelServingConfigServedEntitiesExternalModelCohereConfig",
    "ModelServingConfigServedEntitiesExternalModelCustomProviderConfig",
    "ModelServingConfigServedEntitiesExternalModelCustomProviderConfigApiKeyAuth",
    "ModelServingConfigServedEntitiesExternalModelCustomProviderConfigBearerTokenAuth",
    "ModelServingConfigServedEntitiesExternalModelDatabricksModelServingConfig",
    "ModelServingConfigServedEntitiesExternalModelGoogleCloudVertexAiConfig",
    "ModelServingConfigServedEntitiesExternalModelOpenaiConfig",
    "ModelServingConfigServedEntitiesExternalModelPalmConfig",
    "ModelServingConfigServedModels",
    "ModelServingConfigTrafficConfig",
    "ModelServingConfigTrafficConfigRoutes",
    "ModelServingEmailNotifications",
    "ModelServingRateLimits",
    "ModelServingTags",
    "ModelServingTelemetryConfig",
    "ModelServingTelemetryConfigInferenceTableConfig",
    "ModelServingTelemetryConfigTableNames",
    "ModelServingTimeouts",
]
