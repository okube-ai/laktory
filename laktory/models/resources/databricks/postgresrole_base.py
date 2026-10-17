# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_role
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresRoleProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresRoleSpecAttributes(BaseModel):
    bypassrls: bool | None = Field(None)
    createdb: bool | None = Field(None)
    createrole: bool | None = Field(None)


class PostgresRoleSpec(BaseModel):
    attributes: PostgresRoleSpecAttributes | None = Field(
        None, description="The PG role attributes associated with the role"
    )
    auth_method: str | None = Field(
        None,
        description="Possible values are: `LAKEBASE_OAUTH_V1`, `NO_LOGIN`, `PG_PASSWORD_SCRAM_SHA_256`",
    )
    identity_type: str | None = Field(
        None,
        description="The type of the role. Possible values are: `GROUP`, `SERVICE_PRINCIPAL`, `USER`",
    )
    membership_roles: list[str] | None = Field(
        None,
        description="An enum value for a standard role that this role is a member of",
    )
    postgres_role: str | None = Field(None, description="The name of the Postgres role")


class PostgresRoleBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_role`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    parent: str = Field(
        ...,
        description="The Branch where this Role exists. Format: projects/{project_id}/branches/{branch_id}",
    )
    replace_existing: bool | None = Field(
        None,
        description="If true, update the role if it already exists instead of returning an error",
    )
    role_id: str | None = Field(None, description="Part of the resource name")
    provider_config: PostgresRoleProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresRoleSpec | None = Field(
        None,
        description="The spec contains the role configuration, including identity type, authentication method, and role attributes",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_role"


__all__ = [
    "PostgresRoleBase",
    "PostgresRoleProviderConfig",
    "PostgresRoleSpec",
    "PostgresRoleSpecAttributes",
]
