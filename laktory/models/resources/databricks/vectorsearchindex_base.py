# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_vector_search_index
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class VectorSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumns(BaseModel):
    embedding_model_endpoint_name: str | None = Field(
        None, description="The name of the embedding model endpoint"
    )
    model_endpoint_name_for_query: str | None = Field(
        None,
        description="The name of the embedding model endpoint which, if specified, is used for querying (not ingestion)",
    )
    name: str | None = Field(None, description="The name of the column")


class VectorSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumns(BaseModel):
    embedding_dimension: float | None = Field(
        None, description="Dimension of the embedding vector"
    )
    name: str | None = Field(None, description="The name of the column")


class VectorSearchIndexDeltaSyncIndexSpec(BaseModel):
    embedding_writeback_table: str | None = Field(
        None,
        description="(optional) Automatically sync the vector index contents and computed embeddings to the specified Delta table. The only supported table name is the index name with the suffix `_writeback_table`",
    )
    pipeline_type: str | None = Field(
        None,
        description="Pipeline execution mode. Possible values are: * `TRIGGERED`: If the pipeline uses the triggered execution mode, the system stops processing after successfully refreshing the source table in the pipeline once, ensuring the table is updated based on the data available when the update started. * `CONTINUOUS`: If the pipeline uses continuous execution, the pipeline processes new data as it arrives in the source table to keep the vector index fresh",
    )
    source_table: str | None = Field(None)
    embedding_source_columns: list[
        VectorSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumns
    ] = Field(
        None,
        description="(required if `embedding_vector_columns` isn't provided) array of objects representing columns that contain the embedding source.  Each entry consists of:",
    )
    embedding_vector_columns: list[
        VectorSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumns
    ] = Field(
        None,
        description="(required if `embedding_source_columns` isn't provided)  array of objects representing columns that contain the embedding vectors. Each entry consists of:",
    )


class VectorSearchIndexDirectAccessIndexSpecEmbeddingSourceColumns(BaseModel):
    embedding_model_endpoint_name: str | None = Field(
        None, description="The name of the embedding model endpoint"
    )
    model_endpoint_name_for_query: str | None = Field(
        None,
        description="The name of the embedding model endpoint which, if specified, is used for querying (not ingestion)",
    )
    name: str | None = Field(None, description="The name of the column")


class VectorSearchIndexDirectAccessIndexSpecEmbeddingVectorColumns(BaseModel):
    embedding_dimension: float | None = Field(
        None, description="Dimension of the embedding vector"
    )
    name: str | None = Field(None, description="The name of the column")


class VectorSearchIndexDirectAccessIndexSpec(BaseModel):
    schema_json_: str | None = Field(
        None,
        description="The schema of the index in JSON format.  Check the [API documentation](https://docs.databricks.com/api/workspace/vectorsearchindexes/createindex#direct_access_index_spec-schema_json) for a list of supported data types",
        serialization_alias="schema_json",
    )
    embedding_source_columns: list[
        VectorSearchIndexDirectAccessIndexSpecEmbeddingSourceColumns
    ] = Field(
        None,
        description="(required if `embedding_vector_columns` isn't provided) array of objects representing columns that contain the embedding source.  Each entry consists of:",
    )
    embedding_vector_columns: list[
        VectorSearchIndexDirectAccessIndexSpecEmbeddingVectorColumns
    ] = Field(
        None,
        description="(required if `embedding_source_columns` isn't provided)  array of objects representing columns that contain the embedding vectors. Each entry consists of:",
    )


class VectorSearchIndexTimeouts(BaseModel):
    create: str | None = Field(None)


class VectorSearchIndexBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_vector_search_index`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    endpoint_name: str = Field(
        ...,
        description="(required) The name of the Mosaic AI Vector Search Endpoint that will be used for indexing the data",
    )
    index_type: str = Field(
        ...,
        description="(required) Mosaic AI Vector Search index type. Currently supported values are: * `DELTA_SYNC`: An index that automatically syncs with a source Delta Table, automatically and incrementally updating the index as the underlying data in the Delta Table changes. * `DIRECT_ACCESS`: An index that supports the direct read and write of vectors and metadata through our REST and SDK APIs. With this model, the user manages index updates",
    )
    name: str = Field(..., description="The name of the column")
    primary_key: str = Field(
        ..., description="(required) The column name that will be used as a primary key"
    )
    index_subtype: str | None = Field(None)
    delta_sync_index_spec: VectorSearchIndexDeltaSyncIndexSpec | None = Field(
        None,
        description="(object) Specification for Delta Sync Index. Required if `index_type` is `DELTA_SYNC`. This field is a block and is [documented below](#delta_sync_index_spec-Configuration-Block)",
    )
    direct_access_index_spec: VectorSearchIndexDirectAccessIndexSpec | None = Field(
        None,
        description="(object) Specification for Direct Vector Access Index. Required if `index_type` is `DIRECT_ACCESS`. This field is a block and is [documented below](#direct_access_index_spec-Configuration-Block)",
    )
    timeouts: VectorSearchIndexTimeouts | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_vector_search_index"
