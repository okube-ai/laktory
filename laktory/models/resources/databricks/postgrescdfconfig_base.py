# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_cdf_config
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresCdfConfigProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresCdfConfigBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_cdf_config`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog: str = Field(
        ...,
        description="The Unity Catalog catalog that replicated tables are written into. Set at creation; the CdfConfig is immutable",
    )
    parent: str = Field(
        ...,
        description="The parent database under which to create the CdfConfig. Format: projects/{project}/branches/{branch}/databases/{database}",
    )
    postgres_schema: str = Field(
        ...,
        description="The Postgres schema this CdfConfig replicates from. Unique within the parent database. Set at creation; the CdfConfig is immutable",
    )
    schema_: str = Field(
        ...,
        description="The Unity Catalog schema that replicated tables are written into. Set at creation; the CdfConfig is immutable",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    cdf_config_id: str | None = Field(
        None,
        description="The user-specified id; equals the final segment of `name`. Defaults to the Postgres schema name for configs without an explicit id",
    )
    provider_config: PostgresCdfConfigProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_cdf_config"


__all__ = ["PostgresCdfConfigBase", "PostgresCdfConfigProviderConfig"]
