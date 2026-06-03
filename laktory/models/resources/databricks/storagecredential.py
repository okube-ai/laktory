from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.grants.storagecredentialgrant import StorageCredentialGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.storagecredential_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.storagecredential_base import (
    StorageCredentialBase,
)


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


class StorageCredentialLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Name of the storage credential",
    )


class StorageCredential(StorageCredentialBase):
    """
    Databricks Storage Credential

    Examples
    --------
    ```py
    import io

    from laktory import models

    cred_yaml = '''
    name: prod-azure-mi
    azure_managed_identity:
      access_connector_id: /subscriptions/sub-id/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/connector
    grants:
    - principal: account users
      privileges:
      - READ_FILES
    '''
    cred = models.resources.databricks.StorageCredential.model_validate_yaml(
        io.StringIO(cred_yaml)
    )
    ```

    References
    ----------

    * [Databricks Storage Credential](https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html)
    """

    lookup_existing: StorageCredentialLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing Storage Credential by `name` instead of creating it. The credential becomes available for cross-referencing and child resource deployment (grants, etc.); its own field values are not written to the existing resource.",
    )
    grant: StorageCredentialGrant | list[StorageCredentialGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[StorageCredentialGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Storage Credential - including
    those set outside Laktory - with only the entries listed here. Use only when Laktory owns all access management
    for this resource. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["grant", "grants"]
