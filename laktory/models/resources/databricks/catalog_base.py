# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_catalog
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class CatalogEffectivePredictiveOptimizationFlag(BaseModel):
    inherited_from_name: str | None = Field(None)
    inherited_from_type: str | None = Field(None)
    value: str = Field(...)


class CatalogManagedEncryptionSettingsAzureEncryptionSettings(BaseModel):
    azure_cmk_access_connector_id: str | None = Field(None)
    azure_cmk_managed_identity_id: str | None = Field(None)
    azure_tenant_id: str = Field(...)


class CatalogManagedEncryptionSettings(BaseModel):
    azure_key_vault_key_id: str | None = Field(None)
    customer_managed_key_id: str | None = Field(None)
    azure_encryption_settings: (
        CatalogManagedEncryptionSettingsAzureEncryptionSettings | None
    ) = Field(None)


class CatalogProvisioningInfo(BaseModel):
    state: str | None = Field(None)


class CatalogBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_catalog`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    browse_only: bool | None = Field(
        None,
        description="Indicates whether the principal is limited to retrieving metadata for the associated object through the BROWSE privilege when include_browse is enabled in the request.",
    )
    comment: str | None = Field(
        None, description="User-provided free-form text description."
    )
    connection_name: str | None = Field(
        None, description="The name of the connection to an external data source."
    )
    enable_predictive_optimization: str | None = Field(
        None,
        description="Whether predictive optimization should be enabled for this object and objects under it.",
    )
    force_destroy: bool | None = Field(None)
    isolation_mode: str | None = Field(
        None,
        description="Whether the current securable is accessible from all workspaces or a specific set of workspaces.",
    )
    metastore_id: str | None = Field(
        None, description="Unique identifier of parent metastore."
    )
    name: str | None = Field(None, description="Name of catalog.")
    options_: dict[str, str] | None = Field(
        None,
        description="A map of key-value properties attached to the securable.",
        serialization_alias="options",
    )
    owner: str | None = Field(None, description="Username of current owner of catalog.")
    properties: dict[str, str] | None = Field(
        None, description="A map of key-value properties attached to the securable."
    )
    provider_name: str | None = Field(
        None, description="The name of delta sharing provider."
    )
    share_name: str | None = Field(
        None, description="The name of the share under the share provider."
    )
    storage_root: str | None = Field(
        None, description="Storage root URL for managed tables within catalog."
    )
    effective_predictive_optimization_flag: (
        CatalogEffectivePredictiveOptimizationFlag | None
    ) = Field(None)
    managed_encryption_settings: CatalogManagedEncryptionSettings | None = Field(
        None, description="Control CMK encryption for managed catalog data"
    )
    provisioning_info: CatalogProvisioningInfo | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_catalog"
