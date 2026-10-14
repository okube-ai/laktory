# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_permission_assignment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PermissionAssignmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_permission_assignment`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    permissions: list[str] = Field(
        ..., description="The list of workspace permissions to assign to the principal:"
    )
    group_name: str | None = Field(
        None, description="the group name to assign to a workspace"
    )
    principal_id: int | None = Field(
        None,
        description="Databricks ID of the user, service principal, or group. The principal ID can be retrieved using the account-level SCIM API, or using databricks_user, databricks_service_principal or databricks_group data sources with account API (and has to be an account admin). A more sensible approach is to retrieve the list of `principal_id` as outputs from another Terraform stack",
    )
    service_principal_name: str | None = Field(
        None,
        description="the application ID of service principal to assign to a workspace",
    )
    user_name: str | None = Field(
        None, description="the user name (email) to assign to a workspace"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_permission_assignment"


__all__ = ["PermissionAssignmentBase"]
