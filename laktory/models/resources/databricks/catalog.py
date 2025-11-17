from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.grants.cataloggrant import CatalogGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.schema import Schema
from laktory.models.resources.databricks.workspacebinding import WorkspaceBinding
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class CatalogLookup(ResourceLookup):
    name: str = Field(serialization_alias="id", description="Catalog name")


class CatalogEffectivePredictiveOptimizationFlag(BaseModel):
    """Catalog Effective Predictive Optimization Flag"""

    value: str = Field(..., description="")
    inherited_from_name: str = Field(..., description="")
    inherited_from_type: str = Field(..., description="")


class CatalogProvisioningInfo(BaseModel):
    """Catalog Provisioning Info"""

    state: str = Field(None, description="")


class Catalog(BaseModel, PulumiResource, TerraformResource):
    """
    A catalog is the first layer of Unity Catalog’s three-level namespace. It’s
    used to organize your data assets.

    Examples
    --------
    ```py
    from laktory import models

    catalog = models.resources.databricks.Catalog(
        name="dev",
        grants=[
            {"principal": "account users", "privileges": ["USE_CATALOG", "USE_SCHEMA"]}
        ],
        schemas=[
            {
                "name": "engineering",
                "grants": [
                    {"principal": "domain-engineering", "privileges": ["SELECT"]}
                ],
            },
            {
                "name": "sources",
                "volumes": [
                    {
                        "name": "landing",
                        "volume_type": "EXTERNAL",
                        "grants": [
                            {
                                "principal": "account users",
                                "privileges": ["READ_VOLUME"],
                            },
                            {
                                "principal": "role-metastore-admins",
                                "privileges": ["WRITE_VOLUME"],
                            },
                        ],
                    },
                ],
            },
        ],
    )
    ```

    References
    ----------

    * [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#catalogs)
    * [Pulumi Databricks Catalog](https://www.pulumi.com/registry/packages/databricks/api-docs/catalog/)
    """

    browse_only: bool = Field(None, description="Browse catalog")
    comment: Union[str, None] = Field(
        None, description="Text description of the catalog"
    )
    connection_name: str = Field(
        None,
        description="For Foreign Catalogs: the name of the connection to an external data source. Changes forces creation of a new resource.",
    )
    effective_predictive_optimization_flag: CatalogEffectivePredictiveOptimizationFlag = Field(
        None, description=""
    )
    enable_predictive_optimization: str = Field(
        None,
        description="Whether predictive optimization should be enabled for this object and objects under it. Can be `ENABLE`, `DISABLE` or `INHERIT`",
    )
    force_destroy: bool = Field(
        True, description="If `True` catalog can be deleted, even when not empty"
    )
    foreign_options: dict[str, str] = Field(
        None,
        description="For Foreign Catalogs: the name of the entity from an external data source that maps to a catalog. For example, the database name in a PostgreSQL server.",
    )
    grant: Union[CatalogGrant, list[CatalogGrant]] = Field(
        None,
        description="""
     Grant(s) operating on the Catalog and authoritative for a specific principal. Other principals within the grants 
     are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[CatalogGrant] = Field(
        None,
        description="""
     Grants operating on the Catalog and authoritative for all principals. Replaces any existing grants defined inside 
     or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    isolation_mode: Literal["OPEN", "ISOLATED"] = Field(
        "OPEN",
        description="""
    Whether the catalog is accessible from all workspaces or a specific set of workspaces. Can be ISOLATED or OPEN. 
    Setting the catalog to ISOLATED will automatically allow access from the current workspace.
    """,
    )
    lookup_existing: CatalogLookup = Field(
        None,
        exclude=True,
        description="""
    Specifications for looking up existing resource. Other attributes will be ignored.
    """,
    )
    metastore_id: str = Field(None, description="ID of the parent metastore.")
    name: str = Field(..., description="Name of the catalog")
    owner: str = Field(
        None, description="User/group/service principal name of the catalog owner"
    )
    properties: dict[str, str] = Field(
        None, description="Extensible Catalog properties."
    )
    provider_name: str = Field(
        None,
        description="For Delta Sharing Catalogs: the name of the delta sharing provider. Change forces creation of a new resource.",
    )
    provisioning_info: CatalogProvisioningInfo = Field(None, description="")
    share_name: str = Field(
        None,
        description="For Delta Sharing Catalogs: the name of the share under the share provider. Change forces creation of a new resource.",
    )
    schemas: list[Schema] = Field(
        [], description="List of schemas stored in the catalog"
    )
    storage_root: str = Field(
        None,
        description="""
    Managed location of the catalog. Location in cloud storage where data for managed tables will be stored. If not 
    specified, the location will default to the metastore root location.
    """,
    )
    workspace_bindings: list[WorkspaceBinding] = Field(
        None,
        description="""
    If you use workspaces to isolate user data access, you may want to limit access from
    specific workspaces in your account, also known as workspace binding..
    """,
    )

    @model_validator(mode="after")
    def assign_name(self):
        for schema in self.schemas:
            schema.catalog_name = self.name
            schema.assign_name()

        if self.workspace_bindings:
            for b in self.workspace_bindings:
                b.securable_type = "catalog"
                b.securable_name = self.name

        return self

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Full name of the catalog `{catalog_name}`"""
        return self.name

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - catalog grants
        - schemas resources
        """
        resources = []

        # Catalog grants
        resources += self.get_grants_additional_resources(
            object={"catalog": f"${{resources.{self.resource_name}.id}}"}
        )
        # Catalog schemas
        if self.schemas:
            for s in self.schemas:
                resources += s.core_resources

        # Workspace Bindings
        if self.workspace_bindings:
            resources += self.workspace_bindings

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"foreign_options": "options"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Catalog"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["schemas", "is_unity", "grants", "grant", "workspace_bindings"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_catalog"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
