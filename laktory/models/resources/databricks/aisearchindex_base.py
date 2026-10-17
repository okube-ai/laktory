# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_ai_search_index
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AiSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumns(BaseModel):
    embedding_model_endpoint: str | None = Field(
        None,
        description="Name of the embedding model endpoint, used by default for both ingestion and querying",
    )
    model_endpoint_name_for_query: str | None = Field(
        None,
        description="Name of the embedding model endpoint which, if specified, is used for querying (not ingestion)",
    )
    name: str | None = Field(
        None,
        description="Name of the AI Search index. Server-assigned full resource path (`workspaces/{workspace}/endpoints/{endpoint}/indexes/{index}`) on output, where `{index}` is the index's Unity Catalog table name. On create, the user-supplied UC table name is conveyed via `CreateIndexRequest.index_id`; the server composes the full `name` and returns it on the response",
    )


class AiSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumns(BaseModel):
    embedding_dimension: int | None = Field(
        None, description="Dimension of the embedding vector"
    )
    name: str | None = Field(
        None,
        description="Name of the AI Search index. Server-assigned full resource path (`workspaces/{workspace}/endpoints/{endpoint}/indexes/{index}`) on output, where `{index}` is the index's Unity Catalog table name. On create, the user-supplied UC table name is conveyed via `CreateIndexRequest.index_id`; the server composes the full `name` and returns it on the response",
    )


class AiSearchIndexDeltaSyncIndexSpec(BaseModel):
    columns_to_sync: list[str] | None = Field(
        None,
        description="[Optional] Select the columns to sync with the index. If left blank, all columns from the source table are synced. The primary key column and embedding source or vector column are always synced",
    )
    embedding_source_columns: (
        list[AiSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumns] | None
    ) = Field(None, description="The columns that contain the embedding source")
    embedding_vector_columns: (
        list[AiSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumns] | None
    ) = Field(None, description="The columns that contain the embedding vectors")
    embedding_writeback_table: str | None = Field(
        None,
        description="[Optional] Name of the Delta table to sync the index contents and computed embeddings to",
    )
    pipeline_type: str = Field(
        ...,
        description="Pipeline execution mode. Required on create — the backend rejects an unset value. Storage Optimized endpoints accept only `TRIGGERED`; Standard endpoints accept both. No explicit `stage` — a REQUIRED field staged below its service would be dropped from combined specs while remaining in `required`, tripping the OpenAPI required-vs-properties consistency check. The field inherits the service's launch stage. Possible values are: `CONTINUOUS`, `TRIGGERED`",
    )
    source_table: str | None = Field(
        None, description="The full name of the source Delta table"
    )


class AiSearchIndexDirectAccessIndexSpecEmbeddingSourceColumns(BaseModel):
    embedding_model_endpoint: str | None = Field(
        None,
        description="Name of the embedding model endpoint, used by default for both ingestion and querying",
    )
    model_endpoint_name_for_query: str | None = Field(
        None,
        description="Name of the embedding model endpoint which, if specified, is used for querying (not ingestion)",
    )
    name: str | None = Field(
        None,
        description="Name of the AI Search index. Server-assigned full resource path (`workspaces/{workspace}/endpoints/{endpoint}/indexes/{index}`) on output, where `{index}` is the index's Unity Catalog table name. On create, the user-supplied UC table name is conveyed via `CreateIndexRequest.index_id`; the server composes the full `name` and returns it on the response",
    )


class AiSearchIndexDirectAccessIndexSpecEmbeddingVectorColumns(BaseModel):
    embedding_dimension: int | None = Field(
        None, description="Dimension of the embedding vector"
    )
    name: str | None = Field(
        None,
        description="Name of the AI Search index. Server-assigned full resource path (`workspaces/{workspace}/endpoints/{endpoint}/indexes/{index}`) on output, where `{index}` is the index's Unity Catalog table name. On create, the user-supplied UC table name is conveyed via `CreateIndexRequest.index_id`; the server composes the full `name` and returns it on the response",
    )


class AiSearchIndexDirectAccessIndexSpec(BaseModel):
    embedding_source_columns: (
        list[AiSearchIndexDirectAccessIndexSpecEmbeddingSourceColumns] | None
    ) = Field(None, description="The columns that contain the embedding source")
    embedding_vector_columns: (
        list[AiSearchIndexDirectAccessIndexSpecEmbeddingVectorColumns] | None
    ) = Field(None, description="The columns that contain the embedding vectors")
    schema_json_: str | None = Field(
        None,
        description="The schema of the index in JSON format. Supported types are `integer`, `long`, `float`, `double`, `boolean`, `string`, `date`, `timestamp`. Supported types for vector columns: `array<float>`, `array<double>`",
        serialization_alias="schema_json",
        validation_alias=AliasChoices("schema_json", "schema_json_"),
    )


class AiSearchIndexProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AiSearchIndexBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_ai_search_index`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    index_type: str = Field(
        ...,
        description="Type of index. Required on create and immutable thereafter. Possible values are: `DELTA_SYNC`, `DIRECT_ACCESS`",
    )
    parent: str = Field(
        ...,
        description="The Endpoint where this Index will be created. Format: `workspaces/{workspace_id}/endpoints/{endpoint_id}`",
    )
    primary_key: str = Field(
        ...,
        description="Primary key of the index. Set on create and immutable thereafter",
    )
    index_id: str | None = Field(
        None,
        description="The user-supplied Unity Catalog table name for the Index, per AIP-133. The server composes the full `Index.name` as `{parent}/indexes/{index_id}`. AIP-133 does not list `index_id` as a fields-may-be-required entry, so we annotate it OPTIONAL on the wire; the server still rejects empty values with INVALID_PARAMETER_VALUE",
    )
    index_subtype: str | None = Field(
        None,
        description="The subtype of the index. Set on create and immutable thereafter. Possible values are: `FULL_TEXT`, `HYBRID`, `VECTOR`",
    )
    delta_sync_index_spec: AiSearchIndexDeltaSyncIndexSpec | None = Field(
        None,
        description="Specification for a Delta Sync index. Set when `index_type` is `DELTA_SYNC`",
    )
    direct_access_index_spec: AiSearchIndexDirectAccessIndexSpec | None = Field(
        None,
        description="Specification for a Direct Access index. Set when `index_type` is `DIRECT_ACCESS`",
    )
    provider_config: AiSearchIndexProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_ai_search_index"


__all__ = [
    "AiSearchIndexBase",
    "AiSearchIndexDeltaSyncIndexSpec",
    "AiSearchIndexDeltaSyncIndexSpecEmbeddingSourceColumns",
    "AiSearchIndexDeltaSyncIndexSpecEmbeddingVectorColumns",
    "AiSearchIndexDirectAccessIndexSpec",
    "AiSearchIndexDirectAccessIndexSpecEmbeddingSourceColumns",
    "AiSearchIndexDirectAccessIndexSpecEmbeddingVectorColumns",
    "AiSearchIndexProviderConfig",
]
