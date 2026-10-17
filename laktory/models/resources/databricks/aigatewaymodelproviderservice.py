from laktory.models.resources.databricks.aigatewaymodelproviderservice_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.aigatewaymodelproviderservice_base import (
    AiGatewayModelProviderServiceBase,
)


class AiGatewayModelProviderService(AiGatewayModelProviderServiceBase):
    """
    Databricks AI Gateway model provider service

    Unity Catalog governed connection to an external model provider (OpenAI,
    Azure OpenAI, Anthropic, Amazon Bedrock, Gemini, custom, etc.) and the
    provider models that model services can route to.

    Examples
    --------
    ```py
    import io

    from laktory import models

    service_yaml = '''
    parent: schemas/dev.ai
    model_provider_service_id: openai_prod
    comment: OpenAI production account
    config:
      provider_type: EXTERNAL_MODEL_PROVIDER_TYPE_OPENAI
      targets:
      - model: gpt-5
        native_api_types:
        - openai/v1/chat/completions
      openai:
        direct:
          api_key:
            secret_reference:
              name: secrets/dev.ai.openai_api_key
    '''
    service = (
        models.resources.databricks.AiGatewayModelProviderService.model_validate_yaml(
            io.StringIO(service_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/)
    * [Terraform databricks_ai_gateway_model_provider_service](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/ai_gateway_model_provider_service)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.parent}-{self.model_provider_service_id}"
