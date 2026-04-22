# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_permission_assignment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class MwsPermissionAssignmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_permission_assignment`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    permissions: list[str] = PluralField(
        ...,
        plural="permissionss",
        description="The list of workspace permissions to assign to the principal:",
    )
    principal_id: float = Field(
        ...,
        description="Databricks ID of the user, service principal, or group. The principal ID can be retrieved using the SCIM API, or using [databricks_user](../data-sources/user.md), [databricks_service_principal](../data-sources/service_principal.md) or [databricks_group](../data-sources/group.md) data sources",
    )
    workspace_id: float = Field(..., description="Databricks workspace ID")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_permission_assignment"
