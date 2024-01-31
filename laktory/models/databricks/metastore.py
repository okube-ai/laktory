from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.databricks.metastoreassignment import MetastoreAssignment
from laktory.models.grants.metastoregrant import MetastoreGrant
from laktory.models.databricks.grants import Grants


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
    default_data_access_config_id: str = None
    delta_sharing_organization_name: str = None
    delta_sharing_recipient_token_lifetime_in_seconds: int = None
    delta_sharing_scope: str = None
    force_destroy: bool = None
    global_metastore_id: str = None
    grants: list[MetastoreGrant] = None
    metastore_id: str = None
    name: str = None
    owner: str = None
    region: str = None
    storage_root: str = None
    storage_root_credential_id: str = None
    updated_at: int = None
    updated_by: str = None
    workspace_assignments: list[MetastoreAssignment] = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def core_resources(self) -> list[PulumiResource]:
        """
        # TODO
        - metastore
        - workspace assignments
        - grants
        """
        if self._core_resources is None:
            self._core_resources = [
                self,
            ]
            if self.workspace_assignments:
                for a in self.workspace_assignments:
                    a.metastore_id = f"${{resources.{self.resource_name}.id}}"
                    self._core_resources += [a]

            if self.grants:
                self._core_resources += [
                    Grants(
                        resource_name=f"grants-{self.resource_name}",
                        metastore=f"${{resources.{self.resource_name}.id}}",
                        grants=[
                            {"principal": g.principal, "privileges": g.privileges}
                            for g in self.grants
                        ],
                        options={
                            "depends_on": [f"${{resources.{self.resource_name}}}"]
                            # TODO: Setup workspace provider
                        },
                    )
                ]

        return self._core_resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Metastore"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Metastore

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["workspace_assignments", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_endpoint"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties
        d["channel"] = {"name": d.pop("channel_name")}
        return d
