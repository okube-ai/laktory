# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mlflow_webhook
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MlflowWebhookHttpUrlSpec(BaseModel):
    authorization: str | None = Field(
        None,
        description="Value of the authorization header that should be sent in the request sent by the wehbook.  It should be of the form `<auth type> <credentials>`, e.g. `Bearer <access_token>`. If set to an empty string, no authorization header will be included in the request",
    )
    enable_ssl_verification: bool | None = Field(
        None,
        description="Enable/disable SSL certificate validation. Default is `true`. For self-signed certificates, this field must be `false` AND the destination server must disable certificate validation as well. For security purposes, it is encouraged to perform secret validation with the HMAC-encoded portion of the payload and acknowledge the risk associated with disabling hostname validation whereby it becomes more likely that requests can be maliciously routed to an unintended host",
    )
    secret: str | None = Field(
        None,
        description="Shared secret required for HMAC encoding payload. The HMAC-encoded payload will be sent in the header as `X-Databricks-Signature: encoded_payload`",
    )
    url: str = Field(
        ...,
        description="External HTTPS URL called on event trigger (by using a POST request). Structure of payload depends on the event type, refer to [documentation](https://docs.databricks.com/applications/mlflow/model-registry-webhooks.html) for more details",
    )


class MlflowWebhookJobSpec(BaseModel):
    access_token: str = Field(
        ...,
        description="The personal access token used to authorize webhook's job runs",
    )
    job_id: str = Field(
        ..., description="ID of the Databricks job that the webhook runs"
    )
    workspace_url: str | None = Field(
        None,
        description="URL of the workspace containing the job that this webhook runs. If not specified, the job’s workspace URL is assumed to be the same as the workspace where the webhook is created",
    )


class MlflowWebhookBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mlflow_webhook`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    events: list[str] = Field(
        ...,
        description="The list of events that will trigger execution of Databricks job or POSTing to an URL, for example, `MODEL_VERSION_CREATED`, `MODEL_VERSION_TRANSITIONED_STAGE`, `TRANSITION_REQUEST_CREATED`, etc.  Refer to the [Webhooks API documentation](https://docs.databricks.com/dev-tools/api/latest/mlflow.html#operation/create-registry-webhook) for a full list of supported events",
    )
    description: str | None = Field(
        None, description="Optional description of the MLflow webhook"
    )
    model_name: str | None = Field(
        None,
        description="Name of MLflow model for which webhook will be created. If the model name is not specified, a registry-wide webhook is created that listens for the specified events across all versions of all registered models",
    )
    status: str | None = Field(
        None,
        description="Optional status of webhook. Possible values are `ACTIVE`, `TEST_MODE`, `DISABLED`. Default is `ACTIVE`",
    )
    http_url_spec: MlflowWebhookHttpUrlSpec | None = Field(None)
    job_spec: MlflowWebhookJobSpec | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_webhook"
