# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_default_namespace_setting
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class DefaultNamespaceSettingNamespace(BaseModel):
    value: str | None = Field(None, description="The value for the setting")


class DefaultNamespaceSettingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_default_namespace_setting`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    etag: str | None = Field(None)
    setting_name: str | None = Field(None)
    namespace: DefaultNamespaceSettingNamespace | None = Field(
        None, description="The configuration details"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_default_namespace_setting"


__all__ = ["DefaultNamespaceSettingBase", "DefaultNamespaceSettingNamespace"]
