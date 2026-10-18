# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_workspace_conf
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceConfBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_workspace_conf`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    custom_config: dict[str, str] | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_conf"


__all__ = ["WorkspaceConfBase"]
