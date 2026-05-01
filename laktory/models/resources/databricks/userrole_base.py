# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_user_role
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class UserRoleBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_user_role`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    role: str = Field(
        ...,
        description="Either a role name or the ARN/ID of the [instance profile](instance_profile.md) resource",
    )
    user_id: str = Field(
        ..., description="This is the id of the [user](user.md) resource"
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_user_role"


__all__ = ["UserRoleBase"]
