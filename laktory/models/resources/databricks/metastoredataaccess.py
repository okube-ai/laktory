from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.grants import Grants
from laktory.models.grants.storagecredentialgrant import StorageCredentialGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MetastoreDataAccessAwsIamRole(BaseModel):
    """
    Databricks Metastore Data Access AWS IAM Role

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


class MetastoreDataAccessAzureManagedIdentity(BaseModel):
    """
    Databricks Metastore Data Access Azure Managed Identity

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


class MetastoreDataAccessAzureServicePrincipal(BaseModel):
    """
    Databricks Metastore Data Access Azure Service Principals

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


class MetastoreDataAccessDatabricksGcpServiceAccount(BaseModel):
    """
    Databricks Metastore Data Access GCP Service Account Key

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


class MetastoreDataAccessGcpServiceAccountKey(BaseModel):
    """
    Databricks Metastore Data Access GCP Service Account Key

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


class MetastoreDataAccess(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Metastore Data Access

    Attributes
    ----------
    aws_iam_role:
        AWS IAM role specifications
    azure_managed_identity:
        Azure Managed Identity specifications
    azure_service_principal:
        Azure Service Principal specifications
    comment:
        Comment
    databricks_gcp_service_account:
        Databricks GCP service account specifications
    force_destroy:
        Force resource deletion even if not empty
    force_update:
        Force resource update even if not empty
    grants:
        List of grants operating on the data access
    gcp_service_account_key:
        GCP service account key specifications
    is_default:
        Whether to set this credential as the default for the metastore.
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

    aws_iam_role: MetastoreDataAccessAwsIamRole = None
    azure_managed_identity: MetastoreDataAccessAzureManagedIdentity = None
    azure_service_principal: MetastoreDataAccessAzureServicePrincipal = None
    comment: str = None
    databricks_gcp_service_account: MetastoreDataAccessDatabricksGcpServiceAccount = (
        None
    )
    force_destroy: bool = None
    force_update: bool = None
    gcp_service_account_key: MetastoreDataAccessGcpServiceAccountKey = None
    grants: list[StorageCredentialGrant] = None
    is_default: bool = None
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

        # Catalog grants
        if self.grants:
            grant = Grants(
                resource_name=f"grants-{self.resource_name}",
                storage_credential=f"${{resources.{self.resource_name}.name}}",
                grants=[
                    {"principal": g.principal, "privileges": g.privileges}
                    for g in self.grants
                ],
            )
            resources += [grant]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MetastoreDataAccess"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_metastore_data_access"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
