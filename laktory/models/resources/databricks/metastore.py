from typing import Union

from pydantic import Field

from laktory.models.grants.metastoregrant import MetastoreGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.metastore_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.metastore_base import MetastoreBase
from laktory.models.resources.databricks.metastoreassignment import MetastoreAssignment
from laktory.models.resources.databricks.metastoredataaccess import MetastoreDataAccess


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
    ```
    """

    data_accesses: list[MetastoreDataAccess] = Field(
        None, description="List of data accesses (storage credentials)"
    )
    global_metastore_id: str = Field(None, description="")
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

        if self.data_accesses:
            for data_access in self.data_accesses:
                data_access.metastore_id = f"${{resources.{self.resource_name}.id}}"
                _core_resources = data_access.core_resources
                for r in _core_resources[1:]:
                    if r.options.provider is None:
                        r.options.provider = self.grants_provider
                    if depends_on:
                        r.options.depends_on = depends_on
                resources += _core_resources

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
