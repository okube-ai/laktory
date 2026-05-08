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

    grant: ExternalLocationGrant | list[ExternalLocationGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[ExternalLocationGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this External Location — including
    those set outside Laktory — with only the entries listed here. Use only when Laktory owns all access management
    for this resource. Mutually exclusive with `grant`.
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
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["grant", "grants"]
