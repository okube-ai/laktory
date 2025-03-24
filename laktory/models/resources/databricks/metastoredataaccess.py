from laktory.models.resources.databricks.storagecredential import StorageCredential
from laktory.models.resources.pulumiresource import PulumiResource


class MetastoreDataAccess(StorageCredential):
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
    grant:
        Grant(s) operating on the Metastore Data Access and authoritative for a specific principal.
        Other principals within the grants are preserved. Mutually exclusive with
        `grants`.
    grants:
        Grants operating on the Metastore Data Access and authoritative for all principals.
        Replaces any existing grants defined inside or outside of Laktory. Mutually
        exclusive with `grant`.
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

    is_default: bool = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - metastore data access credential grants
        """
        resources = []

        # Metastore data access grants
        resources += self.get_grants_additional_resources(
            object={"storage_credential": f"${{resources.{self.resource_name}.name}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MetastoreDataAccess"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_metastore_data_access"
