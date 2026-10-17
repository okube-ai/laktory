from laktory.models.resources.databricks.aigatewaymcpservice_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.aigatewaymcpservice_base import (
    AiGatewayMcpServiceBase,
)


class AiGatewayMcpService(AiGatewayMcpServiceBase):
    """
    Databricks AI Gateway MCP service

    Unity Catalog governed access to an MCP server hosted through a Unity
    Catalog connection, with tool selection and rate limits.

    Examples
    --------
    ```py
    import io

    from laktory import models

    service_yaml = '''
    parent: schemas/dev.ai
    mcp_service_id: knowledge_tools
    comment: Governed access to knowledge tools
    config:
      source_connection:
        name: connections/dev.ai.mcp_connection
    '''
    service = models.resources.databricks.AiGatewayMcpService.model_validate_yaml(
        io.StringIO(service_yaml)
    )
    ```

    References
    ----------

    * [Databricks AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/)
    * [Terraform databricks_ai_gateway_mcp_service](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/ai_gateway_mcp_service)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.parent}-{self.mcp_service_id}"
