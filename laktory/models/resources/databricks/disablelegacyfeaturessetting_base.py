# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_disable_legacy_features_setting
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class DisableLegacyFeaturesSettingDisableLegacyFeatures(BaseModel):
    value: bool = Field(...)


class DisableLegacyFeaturesSettingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_disable_legacy_features_setting`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    etag: str | None = Field(None)
    setting_name: str | None = Field(None)
    disable_legacy_features: (
        DisableLegacyFeaturesSettingDisableLegacyFeatures | None
    ) = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_disable_legacy_features_setting"


__all__ = [
    "DisableLegacyFeaturesSettingBase",
    "DisableLegacyFeaturesSettingDisableLegacyFeatures",
]
