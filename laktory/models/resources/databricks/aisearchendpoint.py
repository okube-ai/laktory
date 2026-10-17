from pydantic import Field

from laktory.models.resources.databricks.aisearchendpoint_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.aisearchendpoint_base import (
    AiSearchEndpointBase,
)
from laktory.models.resources.databricks.aisearchindex import AiSearchIndex


class AiSearchEndpoint(AiSearchEndpointBase):
    """
    Databricks AI Search Endpoint

    Compute hosting AI Search indexes. Replacement of `VectorSearchEndpoint`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    endpoint_yaml = '''
    parent: workspaces/1234567890
    endpoint_id: default
    endpoint_type: STANDARD
    indexes:
    - index_id: dev.finance.stock_news_index
      primary_key: id
      index_type: DELTA_SYNC
      delta_sync_index_spec:
        source_table: dev.finance.slv_stock_news
        pipeline_type: TRIGGERED
        embedding_source_columns:
        - name: text
          embedding_model_endpoint: databricks-gte-large-en
    '''
    endpoint = models.resources.databricks.AiSearchEndpoint.model_validate_yaml(
        io.StringIO(endpoint_yaml)
    )
    ```

    References
    ----------

    * [Databricks AI Search](https://docs.databricks.com/aws/en/vector-search/vector-search)
    * [Terraform databricks_ai_search_endpoint](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/ai_search_endpoint)
    """

    # Override description
    parent: str = Field(
        ...,
        description="Workspace in which the endpoint is created, in the format `workspaces/{workspace_id}`",
    )

    # Laktory-specific
    indexes: list[AiSearchIndex] = Field(
        None,
        description="Indexes hosted on the endpoint. Their `parent` is set automatically.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.endpoint_id

    @property
    def additional_core_resources(self) -> list:
        """
        - indexes
        """
        resources = []
        for index in self.indexes or []:
            if index.parent is None:
                index.parent = f"${{resources.{self.resource_name}.name}}"
            resources += [index]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["indexes"]
