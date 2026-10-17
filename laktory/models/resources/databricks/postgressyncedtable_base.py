# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_synced_table
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresSyncedTableProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresSyncedTableSpecExtraColumns(BaseModel):
    column_name: str = Field(
        ...,
        description="Name of the source column whose target PostgreSQL type should be overridden",
    )
    column_type: str = Field(
        ...,
        description="PostgreSQL type of the column, for example 'tsvector' or 'vector(1024)'",
    )
    compute: str | None = Field(
        None,
        description="SQL expression used to compute the column's value, for example 'to_tsvector('english', content)'",
    )
    maintenance: str | None = Field(
        None, description="Possible values are: `STORED_GENERATED`"
    )


class PostgresSyncedTableSpecNewPipelineSpec(BaseModel):
    budget_policy_id: str | None = Field(
        None, description="Budget policy to set on the newly created pipeline"
    )
    pipeline_channel: str | None = Field(
        None,
        description="Release channel of the underlying pipeline's runtime. Some source table configurations (e.g., read-time CDF) require PREVIEW. Defaults to CURRENT if not specified. Possible values are: `CURRENT`, `PREVIEW`",
    )
    storage_catalog: str | None = Field(
        None,
        description="UC catalog for the pipeline to store intermediate files (checkpoints, event logs etc). This needs to be a standard catalog where the user has permissions to create Delta tables",
    )
    storage_schema: str | None = Field(
        None,
        description="UC schema for the pipeline to store intermediate files (checkpoints, event logs etc). This needs to be in the standard catalog where the user has permissions to create Delta tables",
    )


class PostgresSyncedTableSpecTypeOverrides(BaseModel):
    column_name: str = Field(
        ...,
        description="Name of the source column whose target PostgreSQL type should be overridden",
    )
    pg_type: str = Field(
        ...,
        description="PostgreSQL-specific target type to use for the column. Possible values are: `PG_SPECIFIC_TYPE_HALFVEC`, `PG_SPECIFIC_TYPE_VARCHAR`, `PG_SPECIFIC_TYPE_VECTOR`",
    )
    size: int | None = Field(
        None,
        description="Size parameter for the target type, for types that take one (e.g. vector dimension, varchar length). Required when the chosen pg_type needs a size",
    )


class PostgresSyncedTableSpec(BaseModel):
    accelerated_sync: bool | None = Field(
        None,
        description="When true, enables accelerated sync mode for the initial data load. This significantly improves performance for large tables. Requires workspace-level enablement through Lakebase Accelerated Sync preview",
    )
    branch: str | None = Field(
        None, description="The full resource name the branch associated with the table"
    )
    create_database_objects_if_missing: bool | None = Field(
        None,
        description="If true, the synced table's logical database and schema resources in PG will be created if they do not already exist. The request will fail if this is false and the database/schema do not exist",
    )
    existing_pipeline_id: str | None = Field(
        None,
        description="ID of an existing pipeline to bin-pack this synced table into. At most one of existing_pipeline_id and new_pipeline_spec should be defined",
    )
    extra_columns: list[PostgresSyncedTableSpecExtraColumns] | None = Field(
        None, description="Extra PostgreSQL-only columns to add to the synced table"
    )
    new_pipeline_spec: PostgresSyncedTableSpecNewPipelineSpec | None = Field(
        None,
        description="Specification for creating a new pipeline. At most one of existing_pipeline_id and new_pipeline_spec should be defined",
    )
    postgres_database: str | None = Field(
        None,
        description="The Postgres database name where the synced table will be created in",
    )
    primary_key_columns: list[str] | None = Field(
        None,
        description="Primary Key columns to be used for data insert/update in the destination",
    )
    scheduling_policy: str | None = Field(
        None,
        description="Scheduling policy of the underlying pipeline. Possible values are: `CONTINUOUS`, `SNAPSHOT`, `TRIGGERED`",
    )
    source_table_full_name: str | None = Field(
        None,
        description="Three-part (catalog, schema, table) name of the source Delta table",
    )
    timeseries_key: str | None = Field(
        None,
        description="Time series key to deduplicate (tie-break) rows with the same primary key",
    )
    type_overrides: list[PostgresSyncedTableSpecTypeOverrides] | None = Field(
        None,
        description="Override the default Delta->PG type mapping for specific columns. A TypeOverride with PG_SPECIFIC_TYPE_UNSPECIFIED is rejected; a valid pg_type must be set",
    )


class PostgresSyncedTableBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_synced_table`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    synced_table_id: str = Field(
        ...,
        description="The part of the name, chosen by the user when the resource was created",
    )
    provider_config: PostgresSyncedTableProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresSyncedTableSpec | None = Field(
        None,
        description="Configuration details of the synced table, such as the source table, scheduling policy, etc. This attribute is specified at creation time and most fields are returned as is on subsequent queries",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_synced_table"


__all__ = [
    "PostgresSyncedTableBase",
    "PostgresSyncedTableProviderConfig",
    "PostgresSyncedTableSpec",
    "PostgresSyncedTableSpecExtraColumns",
    "PostgresSyncedTableSpecNewPipelineSpec",
    "PostgresSyncedTableSpecTypeOverrides",
]
