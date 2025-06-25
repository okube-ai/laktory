from pydantic import Field

from laktory.models.resources.databricks.storagecredential import StorageCredential
from laktory.models.resources.pulumiresource import PulumiResource


class MetastoreDataAccess(StorageCredential):
    """
    Databricks Metastore Data Access

    Examples
    --------
    ```py
    ```
    """

    is_default: bool = Field(
        None,
        description="Whether to set this credential as the default for the metastore.",
    )

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
