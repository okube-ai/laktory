from typing import Literal
from pydantic import ConfigDict
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MlflowWebhookHttpUrlSpec(BaseModel):
    """
    MLflow webhook URL Specifications

    Attributes
    ----------
    url:
        External HTTPS URL called on event trigger (by using a POST request). Structure of payload depends on the event
        type, refer to [documentation](https://docs.databricks.com/applications/mlflow/model-registry-webhooks.html)
        for more details.
    authorization:
        Value of the authorization header that should be sent in the request sent by the wehbook. It should be of the
        form `<auth type> <credentials>`, e.g. `Bearer <access_token>`. If set to an empty string, no authorization
        header will be included in the request.
    enable_ssl_verification:
        Enable/disable SSL certificate validation. Default is `true`. For self-signed certificates, this field must be
        `false` AND the destination server must disable certificate validation as well. For security purposes, it is
        encouraged to perform secret validation with the HMAC-encoded portion of the payload and acknowledge the risk
        associated with disabling hostname validation whereby it becomes more likely that requests can be maliciously
        routed to an unintended host.
    secret:
        Shared secret required for HMAC encoding payload. The HMAC-encoded payload will be sent in the header as
        `X-Databricks-Signature: encoded_payload`.

    """

    url: str
    authorization: str = None
    enable_ssl_verification: bool = None
    secret: str = None


class MlflowWebhookJobSpec(BaseModel):
    """
    MLflow webhook Job Specifications

    Attributes
    ----------
    access_token:
        The personal access token used to authorize webhook's job runs.
    job_id:
        ID of the Databricks job that the webhook runs.
    workspace_url:
        URL of the workspace containing the job that this webhook runs. If not specified, the job’s workspace URL is
        assumed to be the same as the workspace where the webhook is created.
    """

    access_token: str
    job_id: str
    workspace_url: str = None


class MLflowWebhook(BaseModel, PulumiResource, TerraformResource):
    """
    MLflow Model

    Attributes
    ----------
    events:
        The list of events that will trigger execution of Databricks job or POSTing to an URL, for example,
        `MODEL_VERSION_CREATED`, `MODEL_VERSION_TRANSITIONED_STAGE`, `TRANSITION_REQUEST_CREATED`, etc.
        Refer to the (Webhooks API documentation)[https://docs.databricks.com/dev-tools/api/latest/mlflow.html#operation/create-registry-webhook]
        for a full list of supported events.
    description:
        Optional description of the MLflow webhook.
    http_url_spec:
        URL Specifications
    job_spec:
        Job Specifications
    model_name:
        Name of MLflow model for which webhook will be created. If the model name is not specified, a registry-wide
        webhook is created that listens for the specified events across all versions of all registered models.
    status:
        Optional status of webhook. Default is `ACTIVE`

    Examples
    --------
    ```py
    from laktory import models

    mlwebhook = models.resources.databricks.MLflowWebhook(
        events=["TRANSITION_REQUEST_CREATED"],
        description="Databricks Job webhook trigger",
        status="ACTIVE",
        job_spec={
            "job_id": "some_id",
            "workspace_url": "some_url",
            "access_token": "some_token",
        },
    )
    ```
    """

    model_config = ConfigDict(protected_namespaces=())
    events: list[str]
    description: str = None
    http_url_spec: MlflowWebhookHttpUrlSpec = None
    job_spec: MlflowWebhookJobSpec = None
    model_name: str = None
    status: Literal["ACTIVE", "TEST_MODE", "DISABLED"] = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        key = ""
        if self.model_name:
            key += self.model_name + "-"
        for e in self.events:
            key += e + "-"

        return key.replace(" ", "_")

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    # @property
    # def pulumi_renames(self) -> dict[str, str]:
    #     return {"modelname": "model_name"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MlflowWebhook"

    # @property
    # def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
    #     return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_webhook"

    # @property
    # def terraform_renames(self) -> dict[str, str]:
    #     return self.pulumi_renames

    # @property
    # def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
    #     return self.pulumi_excludes
