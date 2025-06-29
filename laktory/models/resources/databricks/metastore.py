from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.grants.metastoregrant import MetastoreGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.metastoreassignment import MetastoreAssignment
from laktory.models.resources.databricks.metastoredataaccess import MetastoreDataAccess
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MetastoreLookup(ResourceLookup):
    metastore_id: str = Field(
        serialization_alias="id", description="ID of the metastore"
    )


class Metastore(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Metastore

    Examples
    --------
    ```py
    ```
    """

    cloud: str = Field(None, description="")
    created_at: int = Field(None, description="")
    created_by: str = Field(None, description="")
    data_accesses: list[MetastoreDataAccess] = Field(
        None, description="List of data accesses (storage credentials)"
    )
    default_data_access_config_id: str = Field(None, description="")
    delta_sharing_organization_name: str = Field(
        None,
        description="""
    The organization name of a Delta Sharing entity. This field is used for Databricks to Databricks sharing. Once 
    this is set it cannot be removed and can only be modified to another valid value. To delete this value please 
    taint and recreate the resource.
    """,
    )
    delta_sharing_recipient_token_lifetime_in_seconds: int = Field(
        None,
        description="""
    Required along with `delta_sharing_scope`. Used to set expiration duration in seconds on recipient data access 
    tokens. Set to 0 for unlimited duration.
    """,
    )
    delta_sharing_scope: str = Field(
        None,
        description="""
    Required along with delta_sharing_recipient_token_lifetime_in_seconds. Used to enable delta sharing on the 
    metastore. Valid values: INTERNAL, INTERNAL_AND_EXTERNAL.
    """,
    )
    force_destroy: bool = Field(
        None, description="Destroy metastore regardless of its contents."
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
    metastore_id: str = Field(None, description="")
    name: str = Field(None, description="Name of metastore.")
    owner: str = Field(
        None, description="Username/groupname/sp application_id of the metastore owner."
    )
    region: str = Field(None, description="The region of the metastore")
    storage_root: str = Field(
        None,
        description="""
    Path on cloud storage account, where managed databricks.Table are stored. Change forces creation of a new resource.
     If no storage_root is defined for the metastore, each catalog must have a storage_root defined.
    """,
    )
    storage_root_credential_id: Union[str, None] = Field(None, description="")
    updated_at: int = Field(None, description="")
    updated_by: str = Field(None, description="")
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
    def additional_core_resources(self) -> list[PulumiResource]:
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
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Metastore"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "workspace_assignments",
            "grant",
            "grants",
            "grants_provider",
            "data_accesses",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_metastore"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes + ["cloud"]
