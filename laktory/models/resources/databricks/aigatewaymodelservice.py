from laktory.models.resources.databricks.aigatewaymodelservice_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.aigatewaymodelservice_base import (
    AiGatewayModelServiceBase,
)


class AiGatewayModelService(AiGatewayModelServiceBase):
    """
    Databricks AI Gateway model service

    Stable, Unity Catalog governed endpoint routing inference requests to one
    or more destinations (Databricks foundation models, provisioned throughput
    or external model provider services), with rate limits, fallbacks and
    inference tables.

    Examples
    --------
    ```py
    import io

    from laktory import models

    service_yaml = '''
    parent: schemas/dev.ai
    model_service_id: customer_support
    comment: Routes customer support requests
    config:
      routing:
        destinations:
        - name: primary
          destination_type: DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL
          pay_per_token_config:
            model: models/system.ai.databricks-gpt-5
          traffic_percentage: 100
    '''
    service = models.resources.databricks.AiGatewayModelService.model_validate_yaml(
        io.StringIO(service_yaml)
    )
    ```

    References
    ----------

    * [Databricks AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/)
    * [Terraform databricks_ai_gateway_model_service](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/ai_gateway_model_service)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.parent}-{self.model_service_id}"
