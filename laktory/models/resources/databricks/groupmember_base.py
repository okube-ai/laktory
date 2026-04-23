# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_group_member
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class GroupMemberBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_group_member`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    group_id: str = Field(
        ...,
        description="This is the `id` attribute (SCIM ID) of the [group](group.md) resource",
    )
    member_id: str = Field(
        ...,
        description="This is the `id` attribute (SCIM ID) of the [group](group.md), [service principal](service_principal.md), or [user](user.md)",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_group_member"


__all__ = ["GroupMemberBase"]
