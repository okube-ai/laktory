# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_service_principal_role
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ServicePrincipalRoleBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_service_principal_role`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    role: str = Field(
        ...,
        description="This is the role name, role id, or [instance profile](instance_profile.md) resource",
    )
    service_principal_id: str = Field(
        ...,
        description="This is the id of the [service principal](service_principal.md) resource",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal_role"
