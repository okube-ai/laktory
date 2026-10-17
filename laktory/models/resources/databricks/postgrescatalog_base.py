# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_catalog
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresCatalogProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresCatalogSpec(BaseModel):
    branch: str | None = Field(
        None, description="The resource path of the branch associated with the catalog"
    )
    create_database_if_missing: bool | None = Field(
        None,
        description="If set to true, the specified postgres_database is created on behalf of the calling user if it does not already exist. In this case, the calling user has a role created for them in Postgres if they do not already have one",
    )
    postgres_database: str = Field(
        ..., description="The name of the Postgres database associated with the catalog"
    )


class PostgresCatalogBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_catalog`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog_id: str = Field(
        ...,
        description="The part of the name, chosen by the user when the resource was created",
    )
    provider_config: PostgresCatalogProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresCatalogSpec | None = Field(
        None, description="The desired state of the Catalog"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_catalog"


__all__ = [
    "PostgresCatalogBase",
    "PostgresCatalogProviderConfig",
    "PostgresCatalogSpec",
]
