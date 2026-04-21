# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_base_resources.py databricks_schema
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SchemaBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_schema`.
    DO NOT EDIT — regenerate from `approach_a_templates.py`.
    """

    catalog_name: str = Field(...)
    name: str = Field(...)
    comment: str | None = Field(None)
    enable_predictive_optimization: str | None = Field(None)
    force_destroy: bool | None = Field(None)
    metastore_id: str | None = Field(None)
    owner: str | None = Field(None)
    properties: dict[str, str] | None = Field(None)
    storage_root: str | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_schema"
