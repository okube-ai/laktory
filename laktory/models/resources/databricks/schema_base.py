# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_schema
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SchemaBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_schema`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog_name: str = Field(..., description="Name of parent catalog.")
    name: str = Field(..., description="Name of schema, relative to parent catalog.")
    comment: str | None = Field(
        None, description="User-provided free-form text description."
    )
    enable_predictive_optimization: str | None = Field(
        None,
        description="Whether predictive optimization should be enabled for this object and objects under it.",
    )
    force_destroy: bool | None = Field(None)
    metastore_id: str | None = Field(
        None, description="Unique identifier of parent metastore."
    )
    owner: str | None = Field(None, description="Username of current owner of schema.")
    properties: dict[str, str] | None = Field(
        None, description="A map of key-value properties attached to the securable."
    )
    storage_root: str | None = Field(
        None, description="Storage root URL for managed tables within schema."
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_schema"


__all__ = ["SchemaBase"]
