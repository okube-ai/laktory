from typing import Union

from pydantic import Field

from laktory.models.grants.metastoregrant import MetastoreGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.metastore_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.metastore_base import MetastoreBase
from laktory.models.resources.databricks.metastoreassignment import MetastoreAssignment


class MetastoreLookup(ResourceLookup):
    metastore_id: str = Field(
        serialization_alias="id", description="ID of the metastore"
    )


class Metastore(MetastoreBase):
    """
    Databricks Metastore

    Examples
    --------
    ```py
    import io

    from laktory import models

    metastore_yaml = '''
    name: prod
    region: eastus
    grants:
    - principal: account users
      privileges:
      - CREATE_CATALOG
    workspace_assignments:
    - workspace_id: 1234567890
      default_catalog_name: dev
    '''
    metastore = models.resources.databricks.Metastore.model_validate_yaml(
        io.StringIO(metastore_yaml)
    )
    ```

    References
    ----------

    * [Databricks Metastore](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html)
    """

    grant: Union[MetastoreGrant, list[MetastoreGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Metastore and authoritative for a specific principal. Other principals within the grants 
    are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[MetastoreGrant] = Field(
        None,
        description="""
    Grants operating on the Metastore and authoritative for all principals. Replaces any existing grants defined inside
    or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    grants_provider: str = Field(None, description="Provider used for deploying grants")
    lookup_existing: MetastoreLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    workspace_assignments: list[MetastoreAssignment] = Field(
        None, description="List of workspace to which metastore is assigned to"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """
        Resource key used to build default resource name. Equivalent to
        name properties if available. Otherwise, empty string.
        """
        key = self.name
        if key is None and self.lookup_existing:
            key = self.lookup_existing.metastore_id
        return key

    @property
    def additional_core_resources(self) -> list:
        """
        - workspace assignments
        - grants
        """
        resources = []

        depends_on = []
        if self.workspace_assignments:
            for a in self.workspace_assignments:
                a.metastore_id = f"${{resources.{self.resource_name}.id}}"
                depends_on += [f"${{resources.{a.resource_name}}}"]
                resources += [a]

        options = {"provider": self.grants_provider}
        if depends_on:
            options["depends_on"] = depends_on

        _resources = self.get_grants_additional_resources(
            object={"metastore": f"${{resources.{self.resource_name}.id}}"},
            options=options,
        )
        for r in _resources:
            depends_on += [f"${{resources.{r.resource_name}}}"]
        resources += _resources

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "workspace_assignments",
            "grant",
            "grants",
            "grants_provider",
            "data_accesses",
        ]
