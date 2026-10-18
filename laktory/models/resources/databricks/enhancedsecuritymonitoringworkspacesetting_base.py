# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_enhanced_security_monitoring_workspace_setting
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class EnhancedSecurityMonitoringWorkspaceSettingEnhancedSecurityMonitoringWorkspace(
    BaseModel
):
    is_enabled: bool = Field(...)


class EnhancedSecurityMonitoringWorkspaceSettingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_enhanced_security_monitoring_workspace_setting`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    etag: str | None = Field(None)
    setting_name: str | None = Field(None)
    enhanced_security_monitoring_workspace: (
        EnhancedSecurityMonitoringWorkspaceSettingEnhancedSecurityMonitoringWorkspace
        | None
    ) = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_enhanced_security_monitoring_workspace_setting"


__all__ = [
    "EnhancedSecurityMonitoringWorkspaceSettingBase",
    "EnhancedSecurityMonitoringWorkspaceSettingEnhancedSecurityMonitoringWorkspace",
]
