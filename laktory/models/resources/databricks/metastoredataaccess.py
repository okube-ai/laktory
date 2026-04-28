from typing import Union

from pydantic import Field

from laktory.models.grants.storagecredentialgrant import StorageCredentialGrant
from laktory.models.resources.databricks.metastoredataaccess_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.metastoredataaccess_base import (
    MetastoreDataAccessBase,
)


class MetastoreDataAccess(MetastoreDataAccessBase):
    """
    Databricks Metastore Data Access

    Examples
    --------
    ```py
    import io

    from laktory import models

    dac_yaml = '''
    name: prod-azure-mi
    azure_managed_identity:
      access_connector_id: /subscriptions/sub-id/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/connector
    grants:
    - principal: account users
      privileges:
      - READ_FILES
    '''
    dac = models.resources.databricks.MetastoreDataAccess.model_validate_yaml(
        io.StringIO(dac_yaml)
    )
    ```

    References
    ----------

    * [Databricks Metastore Data Access](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/metastore_data_access)
    """

    # Laktory-specific
    grant: Union[StorageCredentialGrant, list[StorageCredentialGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Metastore Data Access and authoritative for a specific principal.
    Other principals within the grants are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[StorageCredentialGrant] = Field(
        None,
        description="""
    Grants operating on the Metastore Data Access and authoritative for all principals.
    Replaces any existing grants defined inside or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - metastore data access credential grants
        """
        resources = []

        resources += self.get_grants_additional_resources(
            object={"storage_credential": f"${{resources.{self.resource_name}.name}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]
