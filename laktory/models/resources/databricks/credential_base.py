# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_credential
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class CredentialAwsIamRole(BaseModel):
    external_id: str | None = Field(None)
    role_arn: str | None = Field(None)
    unity_catalog_iam_arn: str | None = Field(None)


class CredentialAzureManagedIdentity(BaseModel):
    access_connector_id: str = Field(...)
    credential_id: str | None = Field(None)
    managed_identity_id: str | None = Field(None)


class CredentialAzureServicePrincipal(BaseModel):
    application_id: str = Field(...)
    client_secret: str = Field(...)
    directory_id: str = Field(...)


class CredentialDatabricksGcpServiceAccount(BaseModel):
    credential_id: str | None = Field(None)
    email: str | None = Field(None)
    private_key_id: str | None = Field(None)


class CredentialBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_credential`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(...)
    purpose: str = Field(...)
    comment: str | None = Field(None)
    created_at: int | None = Field(None)
    created_by: str | None = Field(None)
    force_destroy: bool | None = Field(None)
    force_update: bool | None = Field(None)
    full_name: str | None = Field(None)
    isolation_mode: str | None = Field(None)
    metastore_id: str | None = Field(None)
    owner: str | None = Field(None)
    read_only: bool | None = Field(None)
    skip_validation: bool | None = Field(None)
    updated_at: int | None = Field(None)
    updated_by: str | None = Field(None)
    used_for_managed_storage: bool | None = Field(None)
    aws_iam_role: CredentialAwsIamRole | None = Field(None)
    azure_managed_identity: CredentialAzureManagedIdentity | None = Field(None)
    azure_service_principal: CredentialAzureServicePrincipal | None = Field(None)
    databricks_gcp_service_account: CredentialDatabricksGcpServiceAccount | None = (
        Field(None)
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_credential"


__all__ = [
    "CredentialAwsIamRole",
    "CredentialAzureManagedIdentity",
    "CredentialAzureServicePrincipal",
    "CredentialBase",
    "CredentialDatabricksGcpServiceAccount",
]
