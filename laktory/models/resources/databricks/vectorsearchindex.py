from typing import Any

from pydantic import model_validator

from laktory.models.resources.databricks.vectorsearchindex_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.vectorsearchindex_base import (
    VectorSearchIndexBase,
)


class VectorSearchIndex(VectorSearchIndexBase):
    """
    Databricks Warehouse

    Examples
    --------
    ```py
    from laktory import models

    index = models.resources.databricks.VectorSearchIndex(
        name="dev.finances.market_news_vs_index",
        primary_key="id",
        endpoint_name="default",
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": "dev.finances.market_news",
            "embedding_vector_columns": [
                {"name": "embedding", "embedding_dimension": 4095}
            ],
            "pipeline_type": "TRIGGERED",
        },
    )
    ```
    """

    # access_controls: list[AccessControl] = []

    @model_validator(mode="after")
    def check_index_spec(self) -> Any:
        if self.index_type == "DELTA_SYNC" and self.delta_sync_index_spec is None:
            raise ValueError(
                "`delta_sync_index_spec` must be set with `index_type` = 'DELTA_SYNC'"
            )

        if self.index_type == "DIRECT_ACCESS" and self.direct_access_index_spec is None:
            raise ValueError(
                "`direct_access_index_spec` must be set with `index_type` = 'DIRECT_ACCESS'"
            )

        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
