from typing import Union

from pydantic import Field

from laktory.models.grants.externallocationgrant import ExternalLocationGrant
from laktory.models.resources.databricks.externallocation_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.externallocation_base import (
    ExternalLocationBase,
)


class ExternalLocation(ExternalLocationBase):
    """
    Databricks External Location

    Examples
    --------
    ```py
    import io

    from laktory import models

    location_yaml = '''
    name: landing
    credential_name: prod-azure-mi
    url: abfss://landing@lakehouse-storage.dfs.core.windows.net/
    comment: External location for raw landing data
    grants:
    - principal: account users
      privileges:
      - READ_FILES
    - principal: role-data-engineers
      privileges:
      - READ_FILES
      - WRITE_FILES
    '''
    location = models.resources.databricks.ExternalLocation.model_validate_yaml(
        io.StringIO(location_yaml)
    )
    ```

    References
    ----------

    * [Databricks External Location](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html)
    """

    grant: Union[ExternalLocationGrant, list[ExternalLocationGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the External Location and authoritative for a specific principal.
    Other principals within the grants are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[ExternalLocationGrant] = Field(
        None,
        description="""
    Grants operating on the External Location and authoritative for all principals. Replaces any existing grants 
    defined inside or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
    @property
    def additional_core_resources(self) -> list:
        """
        - external location grants
        """
        resources = []

        # External Location Grants
        resources += self.get_grants_additional_resources(
            object={"external_location": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]
