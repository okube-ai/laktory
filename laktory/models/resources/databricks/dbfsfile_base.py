# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_dbfs_file
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class DbfsFileBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_dbfs_file`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    content_base64: str | None = Field(
        None,
        description="Encoded file contents. Conflicts with `source`. Use of `content_base64` is discouraged, as it's increasing memory footprint of Terraform state and should only be used in exceptional circumstances, like creating a data pipeline configuration file",
    )
    md5: str | None = Field(None)
    source: str | None = Field(
        None,
        description="The full absolute path to the file. Conflicts with `content_base64`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_dbfs_file"


__all__ = ["DbfsFileBase"]
