from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.grants.storagecredentialgrant import StorageCredentialGrant
from laktory.models.resources.databricks.storagecredential_base import (
    StorageCredentialBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class AwsIamRole(BaseModel):
    role_arn: str = Field(None, description="")
    external_id: str = Field(None, description="")
    unity_catalog_iam_arn: str = Field(None, description="")


class AzureManagedIdentity(BaseModel):
    access_connector_id: str = Field(None, description="")
    credential_id: str = Field(None, description="")
    managed_identity_id: str = Field(None, description="")


class AzureServicePrincipal(BaseModel):
    application_id: str = Field(None, description="")
    client_secret: str = Field(None, description="")
    directory_id: str = Field(None, description="")


class CloudflareApiToken(BaseModel):
    account_id: str = Field(None, description="R2 account ID")
    access_key_id: str = Field(None, description="R2 API token access key ID")
    secret_access_key: str = Field(None, description="R2 API token secret access key")


class DatabricksGcpServiceAccount(BaseModel):
    credential_id: str = Field(None, description="")
    email: str = Field(None, description="")


class GcpServiceAccountKey(BaseModel):
    email: str = Field(None, description="")
    private_key: str = Field(None, description="")
    private_key_id: str = Field(None, description="")


class StorageCredential(StorageCredentialBase, PulumiResource):
    """
    Databricks Storage Credential

    Examples
    --------
    ```py
    ```
    """

    grant: Union[StorageCredentialGrant, list[StorageCredentialGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Storage Credential and authoritative for a specific principal. Other principals within 
    the grants are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[StorageCredentialGrant] = Field(
        None,
        description="""
    Grants operating on the Storage Credential and authoritative for all principals. Replaces any existing grants 
    defined inside or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - storage credential grants
        """
        resources = []

        # Storage Credential grants
        resources += self.get_grants_additional_resources(
            object={"storage_credential": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:StorageCredential"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
