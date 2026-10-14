# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_group_role
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class GroupRoleBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_group_role`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    group_id: str = Field(..., description="This is the id of the group resource")
    role: str = Field(
        ...,
        description="Either a role name or the ARN/ID of the instance profile resource",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_group_role"


__all__ = ["GroupRoleBase"]
