# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_base_resources.py databricks_catalog
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class CatalogEffectivePredictiveOptimizationFlag(BaseModel):
    inherited_from_name: str | None = Field(None)
    inherited_from_type: str | None = Field(None)
    value: str = Field(...)


class CatalogProvisioningInfo(BaseModel):
    state: str | None = Field(None)


class CatalogBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_catalog`.
    DO NOT EDIT — regenerate from `scripts/build_base_resources.py`.
    """

    browse_only: bool | None = Field(None)
    comment: str | None = Field(None)
    connection_name: str | None = Field(None)
    enable_predictive_optimization: str | None = Field(None)
    force_destroy: bool | None = Field(None)
    isolation_mode: str | None = Field(None)
    metastore_id: str | None = Field(None)
    name: str | None = Field(None)
    options_: dict[str, str] | None = Field(None, serialization_alias="options")
    owner: str | None = Field(None)
    properties: dict[str, str] | None = Field(None)
    provider_name: str | None = Field(None)
    share_name: str | None = Field(None)
    storage_root: str | None = Field(None)
    effective_predictive_optimization_flag: (
        CatalogEffectivePredictiveOptimizationFlag | None
    ) = Field(None)
    provisioning_info: CatalogProvisioningInfo | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_catalog"
