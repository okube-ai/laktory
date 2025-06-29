from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class VectorSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumn(BaseModel):
    embedding_model_endpoint_name: str = Field(
        ..., description="The name of the embedding model endpoint"
    )
    name: str = Field(
        ...,
        description="Three-level name of the Vector Search Index to create (catalog.schema.index_name).",
    )


class VectorSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumn(BaseModel):
    embedding_dimension: int = Field(
        ..., description="Dimension of the embedding vector."
    )
    name: str = Field(
        ...,
        description="Three-level name of the Vector Search Index to create (catalog.schema.index_name).",
    )


class VectorSearchIndexDeltaSyncIndexSpec(BaseModel):
    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "embedding_source_columns": "embedding_source_columns",
            "embedding_vector_columns": "embedding_vector_columns",
        }

    embedding_source_columns: list[
        VectorSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumn
    ] = Field(
        None,
        description="Array of objects representing columns that contain the embedding source",
    )
    embedding_vector_columns: list[
        VectorSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumn
    ] = Field(
        None,
        description="""
    (required if embedding_source_columns isn't provided) array of objects representing columns that contain the 
    embedding vectors.
    """,
    )
    embedding_writeback_table: str = Field(None, description="")
    pipeline_id: str = Field(
        None, description="ID of the associated Declarative Pipeline."
    )
    pipeline_type: Literal["TRIGGERED", "CONTINUOUS"] = Field(None, description="")
    source_table: str = Field(
        None,
        description="""
    Pipeline execution mode. Possible values are:
    - TRIGGERED: If the pipeline uses the triggered execution mode, the system stops processing after successfully 
      refreshing the source table in the pipeline once, ensuring the table is updated based on the data available when 
      the update started.
    - CONTINUOUS: If the pipeline uses continuous execution, the pipeline processes new data as it arrives in the 
      source table to keep the vector index fresh.
    """,
    )


class VectorSearchIndexDirectAccessIndexSpec(BaseModel):
    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "embedding_source_columns": "embedding_source_columns",
            "embedding_vector_columns": "embedding_vector_columns",
        }

    embedding_source_columns: list[
        VectorSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumn
    ] = Field(
        None,
        description="Array of objects representing columns that contain the embedding source",
    )
    embedding_vector_columns: list[
        VectorSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumn
    ] = Field(
        None,
        description="""
        (required if embedding_source_columns isn't provided) array of objects representing columns that contain the
        embedding vectors.
        """,
    )
    # schema_json: str


class VectorSearchIndex(BaseModel, PulumiResource, TerraformResource):
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
    endpoint_name: str = Field(
        ...,
        description="The name of the Vector Search Endpoint that will be used for indexing the data.",
    )
    index_type: Literal["DELTA_SYNC", "DIRECT_ACCESS"] = Field(
        ...,
        description="""
    Vector Search index type. Currently supported values are:
        - DELTA_SYNC: An index that automatically syncs with a source Delta Table, automatically and incrementally 
          updating the index as the underlying data in the Delta Table changes.
        - DIRECT_ACCESS: An index that supports the direct read and write of vectors and metadata through our REST and 
          SDK APIs. With this model, the user manages index updates.
    """,
    )
    primary_key: str = Field(
        ..., description="The column name that will be used as a primary key."
    )
    delta_sync_index_spec: VectorSearchIndexDeltaSyncIndexSpec = Field(
        None,
        description="Specification for Delta Sync Index. Required if index_type is DELTA_SYNC.",
    )
    direct_access_index_spec: VectorSearchIndexDirectAccessIndexSpec = Field(
        None,
        description="Specification for Direct Vector Access Index. Required if index_type is DIRECT_ACCESS.",
    )
    name: str = Field(
        ...,
        description="Three-level name of the Vector Search Index to create (catalog.schema.index_name).",
    )

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
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:VectorSearchIndex"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_vector_search_index"

    # @property
    # def terraform_resource_lookup_type(self) -> str:
    #     return "databricks_sql_warehouse"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
