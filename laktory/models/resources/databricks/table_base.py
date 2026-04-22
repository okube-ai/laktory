# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_table
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class TableColumn(BaseModel):
    comment: str | None = Field(None)
    name: str = Field(...)
    nullable: bool | None = Field(None)
    partition_index: float | None = Field(None)
    position: float = Field(...)
    type_interval_type: str | None = Field(None)
    type_json: str | None = Field(None)
    type_name: str = Field(...)
    type_precision: float | None = Field(None)
    type_scale: float | None = Field(None)
    type_text: str = Field(...)


class TableBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_table`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog_name: str = Field(...)
    data_source_format: str = Field(...)
    name: str = Field(...)
    schema_name: str = Field(...)
    table_type: str = Field(...)
    comment: str | None = Field(None)
    owner: str | None = Field(None)
    properties: dict[str, str] | None = Field(None)
    storage_credential_name: str | None = Field(None)
    storage_location: str | None = Field(None)
    view_definition: str | None = Field(None)
    column: list[TableColumn] | None = PluralField(None, plural="columns")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_table"
