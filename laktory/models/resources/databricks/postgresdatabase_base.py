# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_database
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresDatabaseProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresDatabaseSpec(BaseModel):
    postgres_database: str | None = Field(
        None, description="The name of the Postgres database"
    )
    role: str = Field(
        ...,
        description="The name of the role that owns the database. Format: projects/{project_id}/branches/{branch_id}/roles/{role_id}",
    )


class PostgresDatabaseBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_database`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    parent: str = Field(
        ...,
        description="The branch containing this database. Format: projects/{project_id}/branches/{branch_id}",
    )
    database_id: str | None = Field(None, description="Part of the resource name")
    replace_existing: bool | None = Field(
        None,
        description="If true, update the database if it already exists instead of returning an error",
    )
    provider_config: PostgresDatabaseProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresDatabaseSpec | None = Field(
        None, description="The desired state of the Database"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_database"


__all__ = [
    "PostgresDatabaseBase",
    "PostgresDatabaseProviderConfig",
    "PostgresDatabaseSpec",
]
