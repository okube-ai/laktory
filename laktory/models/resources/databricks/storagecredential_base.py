# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_storage_credential
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class StorageCredentialAwsIamRole(BaseModel):
    external_id: str | None = Field(None)
    role_arn: str = Field(...)
    unity_catalog_iam_arn: str | None = Field(None)


class StorageCredentialAzureManagedIdentity(BaseModel):
    access_connector_id: str = Field(...)
    credential_id: str | None = Field(None)
    managed_identity_id: str | None = Field(None)


class StorageCredentialAzureServicePrincipal(BaseModel):
    application_id: str = Field(...)
    client_secret: str = Field(...)
    directory_id: str = Field(...)


class StorageCredentialCloudflareApiToken(BaseModel):
    access_key_id: str = Field(...)
    account_id: str = Field(...)
    secret_access_key: str = Field(...)


class StorageCredentialDatabricksGcpServiceAccount(BaseModel):
    credential_id: str | None = Field(None)
    email: str | None = Field(None)


class StorageCredentialGcpServiceAccountKey(BaseModel):
    email: str = Field(...)
    private_key: str = Field(...)
    private_key_id: str = Field(...)


class StorageCredentialBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_storage_credential`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(...)
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host.",
    )
    comment: str | None = Field(None)
    force_destroy: bool | None = Field(None)
    force_update: bool | None = Field(None)
    isolation_mode: str | None = Field(None)
    metastore_id: str | None = Field(None)
    owner: str | None = Field(None)
    read_only: bool | None = Field(None)
    skip_validation: bool | None = Field(None)
    aws_iam_role: StorageCredentialAwsIamRole | None = Field(None)
    azure_managed_identity: StorageCredentialAzureManagedIdentity | None = Field(None)
    azure_service_principal: StorageCredentialAzureServicePrincipal | None = Field(None)
    cloudflare_api_token: StorageCredentialCloudflareApiToken | None = Field(None)
    databricks_gcp_service_account: (
        StorageCredentialDatabricksGcpServiceAccount | None
    ) = Field(None)
    gcp_service_account_key: StorageCredentialGcpServiceAccountKey | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_storage_credential"


__all__ = [
    "StorageCredentialAwsIamRole",
    "StorageCredentialAzureManagedIdentity",
    "StorageCredentialAzureServicePrincipal",
    "StorageCredentialCloudflareApiToken",
    "StorageCredentialDatabricksGcpServiceAccount",
    "StorageCredentialGcpServiceAccountKey",
    "StorageCredentialBase",
]
