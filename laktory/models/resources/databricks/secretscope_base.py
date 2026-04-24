# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_secret_scope
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SecretScopeKeyvaultMetadata(BaseModel):
    dns_name: str = Field(...)
    resource_id: str = Field(...)


class SecretScopeBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_secret_scope`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ...,
        description="Scope name requested by the user. Must be unique within a workspace. Must consist of alphanumeric characters, dashes, underscores, and periods, and may not exceed 128 characters",
    )
    backend_type: str | None = Field(
        None, description="Either `DATABRICKS` or `AZURE_KEYVAULT`"
    )
    initial_manage_principal: str | None = Field(
        None,
        description="The principal with the only possible value `users` that is initially granted `MANAGE` permission to the created scope.  If it's omitted, then the [databricks_secret_acl](secret_acl.md) with `MANAGE` permission applied to the scope is assigned to the API request issuer's user identity (see [documentation](https://docs.databricks.com/dev-tools/api/latest/secrets.html#create-secret-scope)). This part of the state cannot be imported",
    )
    keyvault_metadata: SecretScopeKeyvaultMetadata | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret_scope"


__all__ = ["SecretScopeKeyvaultMetadata", "SecretScopeBase"]
