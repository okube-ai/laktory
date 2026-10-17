# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_data_classification_catalog_config
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class DataClassificationCatalogConfigAutoTagConfigs(BaseModel):
    auto_tagging_mode: str = Field(
        ...,
        description="Whether auto-tagging is enabled or disabled for this classification tag. Possible values are: `AUTO_TAGGING_DISABLED`, `AUTO_TAGGING_ENABLED`",
    )
    classification_tag: str = Field(
        ...,
        description="The Classification Tag. For built-in classes this is a system tag (e.g., 'class.name', 'class.location'); for custom classes it is a user-defined governance tag key",
    )


class DataClassificationCatalogConfigExcludedSchemas(BaseModel):
    names: list[str] = Field(
        ...,
        description="Schema names, each relative to the parent catalog. Must not be empty",
    )


class DataClassificationCatalogConfigIncludedSchemas(BaseModel):
    names: list[str] = Field(
        ...,
        description="Schema names, each relative to the parent catalog. Must not be empty",
    )


class DataClassificationCatalogConfigProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class DataClassificationCatalogConfigBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_data_classification_catalog_config`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    parent: str = Field(
        ..., description="Parent resource in the format: catalogs/{catalog_name}"
    )
    auto_tag_configs: list[DataClassificationCatalogConfigAutoTagConfigs] | None = (
        Field(
            None,
            description="List of auto-tagging configurations for this catalog. Empty list means no auto-tagging is enabled",
        )
    )
    excluded_schemas: DataClassificationCatalogConfigExcludedSchemas | None = Field(
        None,
        description="Schemas to exclude from the scan, each named relative to the parent catalog. If specified, all schemas except the specified ones will be scanned. Mutually exclusive with `included_schemas`: only one may be set per request. If neither `included_schemas` nor `excluded_schemas` is set, all schemas are scanned",
    )
    included_schemas: DataClassificationCatalogConfigIncludedSchemas | None = Field(
        None,
        description="Schemas to include in the scan, each named relative to the parent catalog. If specified, only listed schemas will be scanned. Mutually exclusive with `excluded_schemas`: only one may be set per request. If neither `included_schemas` nor `excluded_schemas` is set, all schemas are scanned",
    )
    provider_config: DataClassificationCatalogConfigProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_data_classification_catalog_config"


__all__ = [
    "DataClassificationCatalogConfigAutoTagConfigs",
    "DataClassificationCatalogConfigBase",
    "DataClassificationCatalogConfigExcludedSchemas",
    "DataClassificationCatalogConfigIncludedSchemas",
    "DataClassificationCatalogConfigProviderConfig",
]
