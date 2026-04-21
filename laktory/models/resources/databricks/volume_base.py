# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_base_resources.py databricks_volume
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class VolumeBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_volume`.
    DO NOT EDIT — regenerate from `scripts/build_base_resources.py`.
    """

    catalog_name: str = Field(...)
    name: str = Field(...)
    schema_name: str = Field(...)
    volume_type: str = Field(...)
    comment: str | None = Field(None)
    owner: str | None = Field(None)
    storage_location: str | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_volume"
