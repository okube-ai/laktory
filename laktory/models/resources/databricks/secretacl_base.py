# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_secret_acl
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SecretAclBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_secret_acl`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    permission: str = Field(..., description="`READ`, `WRITE` or `MANAGE`")
    principal: str = Field(
        ...,
        description="principal's identifier. It can be: * `user_name` attribute of [databricks_user](user.md). * `display_name` attribute of [databricks_group](group.md).  Use `users` to allow access for all workspace users. * `application_id` attribute of [databricks_service_principal](service_principal.md)",
    )
    scope: str = Field(..., description="name of the scope")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret_acl"
