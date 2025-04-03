from typing import Literal
from typing import Union

from laktory.models.basemodel import BaseModel
from laktory.models.grants.storagecredentialgrant import StorageCredentialGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class AwsIamRole(BaseModel):
    """
    Databricks Storage Credential AWS IAM Role

    Attributes
    ----------
    role_arn:
        todo
    external_id:
        todo
    unity_catalog_iam_arn:
        todo
    """

    role_arn: str = None
    external_id: str = None
    unity_catalog_iam_arn: str = None


class AzureManagedIdentity(BaseModel):
    """
    Databricks Storage Credential Azure Managed Identity

    Attributes
    ----------
    access_connector_id:
        todo
    credential_id:
        todo
    managed_identity_id:
        todo
    """

    access_connector_id: str = None
    credential_id: str = None
    managed_identity_id: str = None


class AzureServicePrincipal(BaseModel):
    """
    Databricks Storage Credential Azure Service Principals

    Attributes
    ----------
    application_id:
        todo
    client_secret:
        todo
    directory_id:
        todo
    """

    application_id: str = None
    client_secret: str = None
    directory_id: str = None


class CloudflareApiToken(BaseModel):
    """
    Databricks Storage Credential Cloudflare API Token

    Attributes
    ----------
    account_id:
        R2 account ID
    access_key_id:
        R2 API token access key ID
    secret_access_key:
        R2 API token secret access key
    """

    account_id: str = None
    access_key_id: str = None
    secret_access_key: str = None


class DatabricksGcpServiceAccount(BaseModel):
    """
    Databricks Storage Credential GCP Service Account Key

    Attributes
    ----------
    email:
        todo
    private_key:
        todo
    private_key_id:
        todo
    """

    credential_id: str = None
    email: str = None


class GcpServiceAccountKey(BaseModel):
    """
    Databricks Storage Credential GCP Service Account Key

    Attributes
    ----------
    email:
        todo
    private_key:
        todo
    private_key_id:
        todo
    """

    email: str = None
    private_key: str = None
    private_key_id: str = None


class StorageCredential(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Storage Credential

    Attributes
    ----------
    aws_iam_role:
        AWS IAM role specifications
    azure_managed_identity:
        Azure Managed Identity specifications
    azure_service_principal:
        Azure Service Principal specifications
    cloudflare_api_token:
        Optional configuration block for using a Cloudflare API Token as credential details.
        This requires account admin access.
    comment:
        Comment
    databricks_gcp_service_account:
        Databricks GCP service account specifications
    force_destroy:
        Force resource deletion even if not empty
    force_update:
        Force resource update even if not empty
    grant:
        Grant(s) operating on the Storage Credential and authoritative for a specific principal.
        Other principals within the grants are preserved. Mutually exclusive with
        `grants`.
    grants:
        Grants operating on the Storage Credential and authoritative for all principals.
        Replaces any existing grants defined inside or outside of Laktory. Mutually
        exclusive with `grant`.
    gcp_service_account_key:
        GCP service account key specifications
    isolation_mode:
        (Optional) Whether the storage credential is accessible from all workspaces or a specific set of workspaces.
        Can be `ISOLATION_MODE_ISOLATED` or `ISOLATION_MODE_OPEN`. Setting the credential to `ISOLATION_MODE_ISOLATED`
        will automatically allow access from the current workspace.
    metastore_id:
        Metastore id
    name:
        Data Access name
    owner:
        Owner
    read_only:
        Read only
    skip_validation:
        Skip Validation

    Examples
    --------
    ```py
    ```
    """

    aws_iam_role: AwsIamRole = None
    azure_managed_identity: AzureManagedIdentity = None
    azure_service_principal: AzureServicePrincipal = None
    cloudflare_api_token: CloudflareApiToken = None
    comment: str = None
    databricks_gcp_service_account: DatabricksGcpServiceAccount = None
    force_destroy: bool = None
    force_update: bool = None
    gcp_service_account_key: GcpServiceAccountKey = None
    grant: Union[StorageCredentialGrant, list[StorageCredentialGrant]] = None
    grants: list[StorageCredentialGrant] = None
    isolation_mode: Literal["ISOLATION_MODE_ISOLATED", "ISOLATION_MODE_OPEN"] = None
    metastore_id: str = None
    name: str = None
    owner: str = None
    read_only: bool = None
    skip_validation: bool = None

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
    def terraform_resource_type(self) -> str:
        return "databricks_storage_credential"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
