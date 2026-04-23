# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_workspace_file
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceFileBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_workspace_file`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    content_base64: str | None = Field(
        None,
        description="The base64-encoded file content. Conflicts with `source`. Use of `content_base64` is discouraged, as it's increasing memory footprint of Terraform state and should only be used in exceptional circumstances, like creating a workspace file with configuration properties for a data pipeline",
    )
    md5: str | None = Field(None)
    object_id: float | None = Field(
        None, description="Unique identifier for a workspace file"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_file"


__all__ = ["WorkspaceFileBase"]
