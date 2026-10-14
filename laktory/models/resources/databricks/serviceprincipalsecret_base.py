# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_service_principal_secret
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ServicePrincipalSecretBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_service_principal_secret`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    service_principal_id: str = Field(
        ...,
        description="SCIM ID of the databricks_service_principal (not application ID)",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )
    create_time: str | None = Field(
        None, description="UTC time when the secret was created"
    )
    expire_time: str | None = Field(
        None,
        description="UTC time when the secret will expire. If the field is not present, the secret does not expire",
    )
    lifetime: str | None = Field(
        None,
        description="The lifetime of the secret in seconds formatted as `NNNNs`. If this parameter is not provided, the secret will have a default lifetime of 730 days (`63072000s`).  Expiration of secret will lead to generation of new secret",
    )
    secret: str | None = Field(
        None, description="**Sensitive** Generated secret for the service principal"
    )
    secret_hash: str | None = Field(None, description="Secret Hash")
    status: str | None = Field(
        None,
        description="Status of the secret (i.e., `ACTIVE` - see [REST API docs for full list](https://docs.databricks.com/api/account/serviceprincipalsecrets/list#secrets-status))",
    )
    time_rotating: str | None = Field(
        None, description="- Changing this argument forces recreation of the secret"
    )
    update_time: str | None = Field(
        None, description="UTC time when the secret was updated"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal_secret"


__all__ = ["ServicePrincipalSecretBase"]
