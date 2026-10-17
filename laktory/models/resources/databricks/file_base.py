# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_file
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class FileBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_file`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    content_base64: str | None = Field(
        None, description="Contents in base 64 format. Conflicts with `source`"
    )
    md5: str | None = Field(None)
    remote_file_modified: bool | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_file"


__all__ = ["FileBase"]
