# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_restrict_workspace_admins_setting
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class RestrictWorkspaceAdminsSettingRestrictWorkspaceAdmins(BaseModel):
    disable_gov_tag_creation: bool | None = Field(None)
    status: str = Field(
        ..., description="The restrict workspace admins status for the workspace"
    )


class RestrictWorkspaceAdminsSettingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_restrict_workspace_admins_setting`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    etag: str | None = Field(None)
    setting_name: str | None = Field(None)
    restrict_workspace_admins: (
        RestrictWorkspaceAdminsSettingRestrictWorkspaceAdmins | None
    ) = Field(None, description="The configuration details")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_restrict_workspace_admins_setting"


__all__ = [
    "RestrictWorkspaceAdminsSettingBase",
    "RestrictWorkspaceAdminsSettingRestrictWorkspaceAdmins",
]
