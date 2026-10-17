# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_data_api
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresDataApiProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresDataApiSpec(BaseModel):
    db_aggregates_enabled: bool | None = Field(
        None, description="Actual aggregate function setting read from the database"
    )
    db_extra_search_path: list[str] | None = Field(
        None, description="Actual extra search path schemas read from the database"
    )
    db_max_rows: int | None = Field(
        None, description="Actual max rows setting read from the database"
    )
    db_schemas: list[str] | None = Field(
        None, description="Actual exposed schemas read from the database"
    )
    jwt_cache_max_lifetime: str | None = Field(
        None, description="Actual JWT cache max lifetime read from the database"
    )
    jwt_role_claim_key: str | None = Field(
        None, description="Actual JWT role claim key read from the database"
    )
    openapi_mode: str | None = Field(
        None,
        description="Actual OpenAPI mode read from the database. Possible values are: `OPEN_API_MODE_DISABLED`, `OPEN_API_MODE_IGNORE_PRIVILEGES`",
    )
    server_cors_allowed_origins: list[str] | None = Field(
        None, description="Actual CORS allowed origins read from the database"
    )
    server_timing_enabled: bool | None = Field(
        None, description="Actual Server-Timing header setting read from the database"
    )


class PostgresDataApiBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_data_api`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    parent: str = Field(
        ...,
        description="The database containing this Data API configuration. Format: projects/{project_id}/branches/{branch_id}/databases/{database_id}",
    )
    provider_config: PostgresDataApiProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresDataApiSpec | None = Field(
        None, description="The desired Data API configuration"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_data_api"


__all__ = [
    "PostgresDataApiBase",
    "PostgresDataApiProviderConfig",
    "PostgresDataApiSpec",
]
