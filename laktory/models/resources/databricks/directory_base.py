# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_directory
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class DirectoryBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_directory`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    path: str = Field(...)
    delete_recursive: bool | None = Field(None)
    object_id: int | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_directory"


__all__ = ["DirectoryBase"]
