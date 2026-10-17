# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_ai_gateway_model_provider_service
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKeySecretAccessKeySecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKeySecretAccessKey(
    BaseModel
):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKeySecretAccessKeySecretReference
        | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKey(BaseModel):
    access_key_id: str | None = Field(
        None,
        description="AWS access key ID. Required on Create when using access-key auth. Treated as username-equivalent (not a secret value): round-trips on reads and is scrubbed from audit logs",
    )
    secret_access_key: (
        AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKeySecretAccessKey
        | None
    ) = Field(
        None,
        description="AWS secret access key paired with `access_key_id`. Required when creating a service with access-key authentication. Supply the value in `secret_access_key.plaintext`",
    )


class AiGatewayModelProviderServiceConfigAmazonBedrockDirectServiceCredential(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigAmazonBedrockDirect(BaseModel):
    aws_access_key: (
        AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKey | None
    ) = Field(
        None,
        description="AWS access-key-pair authentication. Set `access_key_id` and `secret_access_key.plaintext`. Mutually exclusive with `service_credential`",
    )
    region: str | None = Field(
        None,
        description="GCP region of the Gemini Enterprise endpoint (e.g., `us-central1`). Required on Create",
    )
    service_credential: (
        AiGatewayModelProviderServiceConfigAmazonBedrockDirectServiceCredential | None
    ) = Field(
        None,
        description="Reference to a Unity Catalog service credential authorizing Microsoft Foundry requests. On Create, supply `service_credential.name` in the form `credentials/{name}`. Required on Create when using service-credential authentication; mutually exclusive with `api_key` and `entra_service_principal`. The credential is referenced by name; its value is not carried here. Only supported on Azure-hosted workspaces",
    )


class AiGatewayModelProviderServiceConfigAmazonBedrock(BaseModel):
    direct: AiGatewayModelProviderServiceConfigAmazonBedrockDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )


class AiGatewayModelProviderServiceConfigAnthropicDirectApiKeySecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigAnthropicDirectApiKey(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigAnthropicDirectApiKeySecretReference | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigAnthropicDirect(BaseModel):
    api_key: AiGatewayModelProviderServiceConfigAnthropicDirectApiKey | None = Field(
        None,
        description="OpenAI API key. Required when creating the service. Supply the value in `api_key.plaintext`",
    )


class AiGatewayModelProviderServiceConfigAnthropicRelayed(BaseModel):
    pass


class AiGatewayModelProviderServiceConfigAnthropic(BaseModel):
    direct: AiGatewayModelProviderServiceConfigAnthropicDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )
    relayed: AiGatewayModelProviderServiceConfigAnthropicRelayed | None = Field(
        None,
        description="Relayed authentication. Each inference request supplies the caller's OAuth token, which is forwarded to Anthropic. No Anthropic credential is stored. Mutually exclusive with `direct`",
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirectApiKeySecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirectApiKey(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigAzureOpenaiDirectApiKeySecretReference | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipalClientSecretSecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipalClientSecret(
    BaseModel
):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipalClientSecretSecretReference
        | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipal(
    BaseModel
):
    client_id: str | None = Field(
        None, description="Entra ID client (application) ID. Required on Create"
    )
    client_secret: (
        AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipalClientSecret
        | None
    ) = Field(
        None,
        description="Entra ID client secret. Supply the value in `client_secret.plaintext`",
    )
    tenant_id: str | None = Field(
        None, description="Entra ID (Azure AD) tenant ID. Required on Create"
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirectServiceCredential(BaseModel):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigAzureOpenaiDirect(BaseModel):
    api_key: AiGatewayModelProviderServiceConfigAzureOpenaiDirectApiKey | None = Field(
        None,
        description="OpenAI API key. Required when creating the service. Supply the value in `api_key.plaintext`",
    )
    base_url: str | None = Field(
        None,
        description="Optional custom base URL. Defaults to `https://api.openai.com/v1`. Use for OpenAI-API-compatible third-party endpoints or in-network proxies",
    )
    entra_service_principal: (
        AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipal | None
    ) = Field(
        None,
        description="Entra ID service-principal authentication. Set `tenant_id`, `client_id`, and `client_secret.plaintext`. Mutually exclusive with `api_key` and `service_credential`",
    )
    service_credential: (
        AiGatewayModelProviderServiceConfigAzureOpenaiDirectServiceCredential | None
    ) = Field(
        None,
        description="Reference to a Unity Catalog service credential authorizing Microsoft Foundry requests. On Create, supply `service_credential.name` in the form `credentials/{name}`. Required on Create when using service-credential authentication; mutually exclusive with `api_key` and `entra_service_principal`. The credential is referenced by name; its value is not carried here. Only supported on Azure-hosted workspaces",
    )


class AiGatewayModelProviderServiceConfigAzureOpenai(BaseModel):
    direct: AiGatewayModelProviderServiceConfigAzureOpenaiDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )


class AiGatewayModelProviderServiceConfigCustomDirectApiKeySecretReference(BaseModel):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigCustomDirectApiKey(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigCustomDirectApiKeySecretReference | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigCustomDirectHeaderAuthApiKeyValueSecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigCustomDirectHeaderAuthApiKeyValue(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigCustomDirectHeaderAuthApiKeyValueSecretReference
        | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigCustomDirectHeaderAuth(BaseModel):
    api_key_name: str | None = Field(
        None,
        description="HTTP header name that carries the API key on outbound requests (e.g., `Ocp-Apim-Subscription-Key`). The value forwarded under this header is supplied via `api_key_value`",
    )
    api_key_value: (
        AiGatewayModelProviderServiceConfigCustomDirectHeaderAuthApiKeyValue | None
    ) = Field(
        None,
        description="Secret value forwarded under the `api_key_name` header on outbound requests. Supplied as inline plaintext via `ProviderSecret.plaintext`",
    )


class AiGatewayModelProviderServiceConfigCustomDirect(BaseModel):
    api_key: AiGatewayModelProviderServiceConfigCustomDirectApiKey | None = Field(
        None,
        description="OpenAI API key. Required when creating the service. Supply the value in `api_key.plaintext`",
    )
    base_url: str | None = Field(
        None,
        description="Optional custom base URL. Defaults to `https://api.openai.com/v1`. Use for OpenAI-API-compatible third-party endpoints or in-network proxies",
    )
    header_auth: AiGatewayModelProviderServiceConfigCustomDirectHeaderAuth | None = (
        Field(
            None,
            description="Header-based API-key auth: the secret is forwarded on outbound requests under a caller-chosen HTTP header rather than as an `Authorization` bearer token. Set this instead of `api_key` for header auth",
        )
    )


class AiGatewayModelProviderServiceConfigCustom(BaseModel):
    direct: AiGatewayModelProviderServiceConfigCustomDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )


class AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectApiKeySecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectApiKey(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectApiKeySecretReference
        | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectServiceCredential(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigGeminiEnterpriseDirect(BaseModel):
    api_key: AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectApiKey | None = (
        Field(
            None,
            description="OpenAI API key. Required when creating the service. Supply the value in `api_key.plaintext`",
        )
    )
    project_id: str | None = Field(
        None,
        description="GCP project ID hosting the Gemini Enterprise endpoint. Required on Create",
    )
    region: str | None = Field(
        None,
        description="GCP region of the Gemini Enterprise endpoint (e.g., `us-central1`). Required on Create",
    )
    service_credential: (
        AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectServiceCredential
        | None
    ) = Field(
        None,
        description="Reference to a Unity Catalog service credential authorizing Microsoft Foundry requests. On Create, supply `service_credential.name` in the form `credentials/{name}`. Required on Create when using service-credential authentication; mutually exclusive with `api_key` and `entra_service_principal`. The credential is referenced by name; its value is not carried here. Only supported on Azure-hosted workspaces",
    )


class AiGatewayModelProviderServiceConfigGeminiEnterprise(BaseModel):
    direct: AiGatewayModelProviderServiceConfigGeminiEnterpriseDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )


class AiGatewayModelProviderServiceConfigInferenceTable(BaseModel):
    parent: str = Field(
        ...,
        description="Parent Unity Catalog schema where the inference table is created, in the form `schemas/{catalog}.{schema}`. Required when configuring an inference table. After the inference table is created, this field cannot be changed",
    )
    table_name_prefix: str | None = Field(
        None,
        description="Prefix used to form the inference table's registered name. AI Gateway appends `_payload`; for example, `table_name_prefix = 'orders'` creates `orders_payload`. If unset, the prefix defaults to the service name. Read `table` from the response for the resulting resource name. After the inference table is created, this field cannot be changed",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectApiKeySecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectApiKey(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectApiKeySecretReference
        | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipalClientSecretSecretReference(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipalClientSecret(
    BaseModel
):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipalClientSecretSecretReference
        | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipal(
    BaseModel
):
    client_id: str | None = Field(
        None, description="Entra ID client (application) ID. Required on Create"
    )
    client_secret: (
        AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipalClientSecret
        | None
    ) = Field(
        None,
        description="Entra ID client secret. Supply the value in `client_secret.plaintext`",
    )
    tenant_id: str | None = Field(
        None, description="Entra ID (Azure AD) tenant ID. Required on Create"
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectServiceCredential(
    BaseModel
):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundryDirect(BaseModel):
    api_key: AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectApiKey | None = (
        Field(
            None,
            description="OpenAI API key. Required when creating the service. Supply the value in `api_key.plaintext`",
        )
    )
    base_url: str | None = Field(
        None,
        description="Optional custom base URL. Defaults to `https://api.openai.com/v1`. Use for OpenAI-API-compatible third-party endpoints or in-network proxies",
    )
    entra_service_principal: (
        AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipal
        | None
    ) = Field(
        None,
        description="Entra ID service-principal authentication. Set `tenant_id`, `client_id`, and `client_secret.plaintext`. Mutually exclusive with `api_key` and `service_credential`",
    )
    service_credential: (
        AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectServiceCredential
        | None
    ) = Field(
        None,
        description="Reference to a Unity Catalog service credential authorizing Microsoft Foundry requests. On Create, supply `service_credential.name` in the form `credentials/{name}`. Required on Create when using service-credential authentication; mutually exclusive with `api_key` and `entra_service_principal`. The credential is referenced by name; its value is not carried here. Only supported on Azure-hosted workspaces",
    )


class AiGatewayModelProviderServiceConfigMicrosoftFoundry(BaseModel):
    direct: AiGatewayModelProviderServiceConfigMicrosoftFoundryDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )


class AiGatewayModelProviderServiceConfigOpenaiDirectApiKeySecretReference(BaseModel):
    name: str = Field(
        ...,
        description="Resource name of the provider service. Format: `model-provider-services/{catalog}.{schema}.{model_provider_service}`. Each `{...}` component is capped at 255 characters individually. Server-derived on Create from `parent` + `model_provider_service_id`; required and immutable on Update/Get/Delete",
    )


class AiGatewayModelProviderServiceConfigOpenaiDirectApiKey(BaseModel):
    plaintext: str | None = Field(
        None,
        description="Inline plaintext credential. INPUT_ONLY: the value never round-trips on reads. Get and List responses omit `plaintext`; the enclosing secret object remains present to indicate that a secret is configured",
    )
    secret_reference: (
        AiGatewayModelProviderServiceConfigOpenaiDirectApiKeySecretReference | None
    ) = Field(
        None,
        description="Reference to a customer-owned UC Secret that carries this secret value. The value is read at invoke time under the model provider service owner's access and is never copied onto the model provider service, so rotating the UC Secret takes effect with no change to the model provider service. On Create, supply `secret_reference.name` as `secrets/{catalog}.{schema}.{secret}`",
    )


class AiGatewayModelProviderServiceConfigOpenaiDirect(BaseModel):
    api_key: AiGatewayModelProviderServiceConfigOpenaiDirectApiKey | None = Field(
        None,
        description="OpenAI API key. Required when creating the service. Supply the value in `api_key.plaintext`",
    )
    base_url: str | None = Field(
        None,
        description="Optional custom base URL. Defaults to `https://api.openai.com/v1`. Use for OpenAI-API-compatible third-party endpoints or in-network proxies",
    )
    organization: str | None = Field(
        None,
        description="Optional OpenAI organization ID. When set, the platform forwards it as the `OpenAI-Organization` header",
    )


class AiGatewayModelProviderServiceConfigOpenai(BaseModel):
    direct: AiGatewayModelProviderServiceConfigOpenaiDirect | None = Field(
        None, description="OpenAI configuration with an API key supplied in the request"
    )


class AiGatewayModelProviderServiceConfigRateLimits(BaseModel):
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


class AiGatewayModelProviderServiceConfigTargets(BaseModel):
    model: str = Field(
        ...,
        description="Provider-side model identifier, such as `gpt-5` or `claude-opus-4-7`. This identifies a model at the upstream provider; it is not a Unity Catalog model resource",
    )
    native_api_types: list[str] | None = Field(
        None,
        description="Provider-native API types supported by this model, such as `openai/v1/chat/completions`. At least one value is required. AI Gateway uses these values to translate requests and responses. At most 64 entries of 256 characters each are allowed",
    )


class AiGatewayModelProviderServiceConfig(BaseModel):
    allow_all_targets: bool | None = Field(
        None,
        description="When true, accepts any model exposed by the upstream provider; `targets` is not required and does not restrict routability. When false, only models listed in `targets` are routable. Defaults to false * `amazon_bedrock` (ModelProviderServiceConfigAmazonBedrockProviderConfig, optional) * `anthropic` (ModelProviderServiceConfigAnthropicProviderConfig, optional) * `azure_openai` (ModelProviderServiceConfigAzureOpenAiProviderConfig, optional) * `custom` (ModelProviderServiceConfigCustomProviderConfig, optional)",
    )
    amazon_bedrock: AiGatewayModelProviderServiceConfigAmazonBedrock | None = Field(
        None
    )
    anthropic: AiGatewayModelProviderServiceConfigAnthropic | None = Field(None)
    azure_openai: AiGatewayModelProviderServiceConfigAzureOpenai | None = Field(None)
    custom: AiGatewayModelProviderServiceConfigCustom | None = Field(None)
    forward_headers: bool | None = Field(
        None,
        description="Whether to forward incoming HTTP headers to the upstream provider. Defaults to false and is configured for the entire provider service, not per request. Upstream authentication is configured separately in the provider-specific configuration",
    )
    forward_query_parameters: bool | None = Field(
        None,
        description="Whether to forward incoming query parameters to the upstream provider. Defaults to false and is configured for the entire provider service, not per request",
    )
    forward_unmanaged_paths: bool | None = Field(
        None,
        description="Whether to proxy paths that AI Gateway does not recognize as configured provider-native API types. Defaults to false. When true, these paths are forwarded unchanged to the upstream provider. When false, only recognized API paths are served. Enabling this broadens the upstream API surface exposed through the provider service * `gemini_enterprise` (ModelProviderServiceConfigGeminiEnterpriseProviderConfig, optional)",
    )
    gemini_enterprise: AiGatewayModelProviderServiceConfigGeminiEnterprise | None = (
        Field(None)
    )
    inference_table: AiGatewayModelProviderServiceConfigInferenceTable | None = Field(
        None,
        description="Payload logging configuration for requests sent directly to this provider service. Requests routed through a model service are captured by that model service's inference table instead * `microsoft_foundry` (ModelProviderServiceConfigMicrosoftFoundryProviderConfig, optional) * `openai` (ModelProviderServiceConfigOpenAiProviderConfig, optional)",
    )
    microsoft_foundry: AiGatewayModelProviderServiceConfigMicrosoftFoundry | None = (
        Field(None)
    )
    openai: AiGatewayModelProviderServiceConfigOpenai | None = Field(None)
    provider_type: str | None = Field(
        None,
        description="External model provider. Required on Create and immutable thereafter. Set the matching provider-specific configuration, such as `openai`, `azure_openai`, or `amazon_bedrock`. Possible values are: `EXTERNAL_MODEL_PROVIDER_TYPE_AMAZON_BEDROCK`, `EXTERNAL_MODEL_PROVIDER_TYPE_ANTHROPIC`, `EXTERNAL_MODEL_PROVIDER_TYPE_AZURE_OPENAI`, `EXTERNAL_MODEL_PROVIDER_TYPE_CUSTOM`, `EXTERNAL_MODEL_PROVIDER_TYPE_GEMINI_ENTERPRISE`, `EXTERNAL_MODEL_PROVIDER_TYPE_MICROSOFT_FOUNDRY`, `EXTERNAL_MODEL_PROVIDER_TYPE_OPENAI`",
    )
    rate_limits: list[AiGatewayModelProviderServiceConfigRateLimits] | None = Field(
        None,
        description="Rate limits for requests sent directly to this provider service. Requests routed through a model service use that model service's rate limits instead",
    )
    targets: list[AiGatewayModelProviderServiceConfigTargets] | None = Field(
        None,
        description="Models and provider-native API types exposed by this provider service. Each entry must include at least one `native_api_types` value. When `allow_all_targets` is false, at least one entry is required and model service destinations can reference only listed models. When `allow_all_targets` is true, any upstream model is routable; entries in this list provide API-type metadata without restricting other models",
    )


class AiGatewayModelProviderServiceProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AiGatewayModelProviderServiceBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_ai_gateway_model_provider_service`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    model_provider_service_id: str = Field(
        ..., description="Name for the model provider service, e.g. 'openai_prod'"
    )
    parent: str = Field(
        ...,
        description="Parent Unity Catalog schema where the inference table is created, in the form `schemas/{catalog}.{schema}`. Required when configuring an inference table. After the inference table is created, this field cannot be changed",
    )
    comment: str | None = Field(None, description="User-provided description")
    config: AiGatewayModelProviderServiceConfig | None = Field(
        None,
        description="Provider authentication, exposed models, request-forwarding controls, rate limits, and payload logging. Required on Create. On Update, it is required only when `config` or one of its subpaths appears in `update_mask`",
    )
    provider_config: AiGatewayModelProviderServiceProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_ai_gateway_model_provider_service"


__all__ = [
    "AiGatewayModelProviderServiceBase",
    "AiGatewayModelProviderServiceConfig",
    "AiGatewayModelProviderServiceConfigAmazonBedrock",
    "AiGatewayModelProviderServiceConfigAmazonBedrockDirect",
    "AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKey",
    "AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKeySecretAccessKey",
    "AiGatewayModelProviderServiceConfigAmazonBedrockDirectAwsAccessKeySecretAccessKeySecretReference",
    "AiGatewayModelProviderServiceConfigAmazonBedrockDirectServiceCredential",
    "AiGatewayModelProviderServiceConfigAnthropic",
    "AiGatewayModelProviderServiceConfigAnthropicDirect",
    "AiGatewayModelProviderServiceConfigAnthropicDirectApiKey",
    "AiGatewayModelProviderServiceConfigAnthropicDirectApiKeySecretReference",
    "AiGatewayModelProviderServiceConfigAnthropicRelayed",
    "AiGatewayModelProviderServiceConfigAzureOpenai",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirect",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirectApiKey",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirectApiKeySecretReference",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipal",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipalClientSecret",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirectEntraServicePrincipalClientSecretSecretReference",
    "AiGatewayModelProviderServiceConfigAzureOpenaiDirectServiceCredential",
    "AiGatewayModelProviderServiceConfigCustom",
    "AiGatewayModelProviderServiceConfigCustomDirect",
    "AiGatewayModelProviderServiceConfigCustomDirectApiKey",
    "AiGatewayModelProviderServiceConfigCustomDirectApiKeySecretReference",
    "AiGatewayModelProviderServiceConfigCustomDirectHeaderAuth",
    "AiGatewayModelProviderServiceConfigCustomDirectHeaderAuthApiKeyValue",
    "AiGatewayModelProviderServiceConfigCustomDirectHeaderAuthApiKeyValueSecretReference",
    "AiGatewayModelProviderServiceConfigGeminiEnterprise",
    "AiGatewayModelProviderServiceConfigGeminiEnterpriseDirect",
    "AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectApiKey",
    "AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectApiKeySecretReference",
    "AiGatewayModelProviderServiceConfigGeminiEnterpriseDirectServiceCredential",
    "AiGatewayModelProviderServiceConfigInferenceTable",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundry",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirect",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectApiKey",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectApiKeySecretReference",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipal",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipalClientSecret",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectEntraServicePrincipalClientSecretSecretReference",
    "AiGatewayModelProviderServiceConfigMicrosoftFoundryDirectServiceCredential",
    "AiGatewayModelProviderServiceConfigOpenai",
    "AiGatewayModelProviderServiceConfigOpenaiDirect",
    "AiGatewayModelProviderServiceConfigOpenaiDirectApiKey",
    "AiGatewayModelProviderServiceConfigOpenaiDirectApiKeySecretReference",
    "AiGatewayModelProviderServiceConfigRateLimits",
    "AiGatewayModelProviderServiceConfigTargets",
    "AiGatewayModelProviderServiceProviderConfig",
]
