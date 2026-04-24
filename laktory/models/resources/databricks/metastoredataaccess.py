from typing import Union

from pydantic import Field

from laktory.models.grants.storagecredentialgrant import StorageCredentialGrant
from laktory.models.resources.databricks.metastoredataaccess_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.metastoredataaccess_base import (
    MetastoreDataAccessBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class MetastoreDataAccess(MetastoreDataAccessBase, PulumiResource):
    """
    Databricks Metastore Data Access

    Examples
    --------
    ```py
    ```
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
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - metastore data access credential grants
        """
        resources = []

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

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
