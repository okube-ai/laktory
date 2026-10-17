from pydantic import Field

from laktory.models.resources.databricks.aisearchindex_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.aisearchindex_base import AiSearchIndexBase


class AiSearchIndex(AiSearchIndexBase):
    """
    Databricks AI Search Index

    Searchable collection of records hosted on an AI Search endpoint, kept in
    sync with a source Delta table (`DELTA_SYNC`) or written to through the API
    (`DIRECT_ACCESS`). Replacement of `VectorSearchIndex`. Can also be declared
    directly in the `indexes` of an `AiSearchEndpoint`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    index_yaml = '''
    parent: workspaces/1234567890/endpoints/default
    index_id: dev.finance.stock_news_index
    primary_key: id
    index_type: DELTA_SYNC
    delta_sync_index_spec:
      source_table: dev.finance.slv_stock_news
      pipeline_type: TRIGGERED
      embedding_source_columns:
      - name: text
        embedding_model_endpoint: databricks-gte-large-en
    '''
    index = models.resources.databricks.AiSearchIndex.model_validate_yaml(
        io.StringIO(index_yaml)
    )
    ```

    References
    ----------

    * [Databricks AI Search](https://docs.databricks.com/aws/en/vector-search/vector-search)
    * [Terraform databricks_ai_search_index](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/ai_search_index)
    """

    # Relax field the base marks required but Laktory fills from the parent endpoint
    parent: str = Field(
        None,
        description="Endpoint hosting the index, in the format `workspaces/{workspace_id}/endpoints/{endpoint_id}`. Set automatically when declared in the indexes of an endpoint.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.index_id
