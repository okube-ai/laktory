from typing import Union
from pydantic import Field
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.metastoreassignment import MetastoreAssignment
from laktory.models.resources.databricks.metastoredataaccess import MetastoreDataAccess
from laktory.models.grants.metastoregrant import MetastoreGrant
from laktory.models.resources.databricks.grants import Grants


class MetastoreLookup(ResourceLookup):
    """
    Attributes
    ----------
    metastore_id:
        ID of the metastore
    """

    metastore_id: str = Field(serialization_alias="id")


class Metastore(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Metastore

    Attributes
    ----------
    cloud:
        todo
    created_at:
        todo
    created_by:
        todo
    default_data_access_config_id:
        todo
    delta_sharing_organization_name:
        The organization name of a Delta Sharing entity. This field is used for
         Databricks to Databricks sharing. Once this is set it cannot be
        removed and can only be modified to another valid value. To delete
        this value please taint and recreate the resource.
    delta_sharing_recipient_token_lifetime_in_seconds:
        Required along with `delta_sharing_scope`. Used to set expiration
        duration in seconds on recipient data access tokens.
        Set to 0 for unlimited duration.
    delta_sharing_scope:
        Required along with delta_sharing_recipient_token_lifetime_in_seconds.
         Used to enable delta sharing on the metastore.
        Valid values: INTERNAL, INTERNAL_AND_EXTERNAL.
    force_destroy:
        Destroy metastore regardless of its contents.
    global_metastore_id:
        todo
    grants:
        List of grants operating on the metastore
    grants_provider:
        Provider used for deploying grants
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    metastore_id:
        todo
    name:
        Name of metastore.
    owner:
        Username/groupname/sp application_id of the metastore owner.
    region:
        The region of the metastore
    storage_root:
        Path on cloud storage account, where managed databricks.Table are
        stored. Change forces creation of a new resource. If no storage_root is
        defined for the metastore, each catalog must have a storage_root
        defined.
    storage_root_credential_id:
        todo
    updated_at:
        todo
    updated_by:
        todo
    workspace_assignments:
        List of workspace to which metastore is assigned to

    Examples
    --------
    ```py
    ```
    """

    cloud: str = None
    created_at: int = None
    created_by: str = None
    data_accesses: list[MetastoreDataAccess] = None
    default_data_access_config_id: str = None
    delta_sharing_organization_name: str = None
    delta_sharing_recipient_token_lifetime_in_seconds: int = None
    delta_sharing_scope: str = None
    force_destroy: bool = None
    global_metastore_id: str = None
    grants: list[MetastoreGrant] = None
    grants_provider: str = None
    lookup_existing: MetastoreLookup = Field(None, exclude=True)
    metastore_id: str = None
    name: str = None
    owner: str = None
    region: str = None
    storage_root: str = None
    storage_root_credential_id: Union[str, None] = None
    updated_at: int = None
    updated_by: str = None
    workspace_assignments: list[MetastoreAssignment] = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

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

        if self.grants:
            options = {"provider": self.grants_provider}
            if depends_on:
                options["depends_on"] = depends_on

            resources += Grants(
                resource_name=f"grants-{self.resource_name}",
                metastore=f"${{resources.{self.resource_name}.id}}",
                grants=[
                    {"principal": g.principal, "privileges": g.privileges}
                    for g in self.grants
                ],
                options=options,
            ).core_resources

            depends_on += [f"${{resources.{resources[-1].resource_name}}}"]

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
