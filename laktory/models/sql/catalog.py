from typing import Union
from typing import Literal
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.sql.schema import Schema
from laktory.models.grants.cataloggrant import CatalogGrant
from laktory.models.databricks.grants import Grants


class Catalog(BaseModel, PulumiResource):
    """
    A catalog is the first layer of Unity Catalog’s three-level namespace. It’s
    used to organize your data assets.

    Attributes
    ----------
    comment:
        Text description of the catalog
    force_destroy:
        If `True` catalog can be deleted, even when not empty
    grants:
        List of grants operating on the catalog
    isolation_mode:
        Whether the catalog is accessible from all workspaces or a specific set
        of workspaces. Can be ISOLATED or OPEN. Setting the catalog to ISOLATED
        will automatically allow access from the current workspace.
    name:
        Name of the catalog
    owner:
        User/group/service principal name of the catalog owner
    schemas:
        List of schemas stored in the catalog
    storage_root:
        Managed location of the catalog. Location in cloud storage where data
        for managed tables will be stored. If not specified, the location will
        default to the metastore root location.

    Examples
    --------
    ```py
    from laktory import models

    catalog = models.Catalog(
        name="dev",
        grants=[
            {"principal": "account users", "privileges": ["USE_CATALOG", "USE_SCHEMA"]}
        ],
        schemas=[
            {
                "name": "engineering",
                "grants": [{"principal": "domain-engineering", "privileges": ["SELECT"]}],
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
    catalog.to_pulumi()
    ```

    References
    ----------

    * [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#catalogs)
    * [Pulumi Databricks Catalog](https://www.pulumi.com/registry/packages/databricks/api-docs/catalog/)
    """

    comment: Union[str, None] = None
    force_destroy: bool = True
    grants: list[CatalogGrant] = None
    isolation_mode: Union[Literal["OPEN", "ISOLATED"], str] = "OPEN"
    name: str
    owner: str = None
    schemas: list[Schema] = []
    storage_root: str = None

    def model_post_init(self, __context):
        for schema in self.schemas:
            schema.catalog_name = self.name
            schema.model_post_init(None)

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
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [self]

            # Catalog grants
            if self.grants:
                self.resources_ += [
                    Grants(
                        resource_name=f"grants-{self.resource_name}",
                        catalog=self.full_name,
                        grants=[
                            {"principal": g.principal, "privileges": g.privileges}
                            for g in self.grants
                        ],
                        options={
                            "depends_on": [f"${{resources.{self.resource_name}}}"]
                        },
                    )
                ]

            if self.schemas:
                for s in self.schemas:
                    self.resources_ += s.resources
                    # TODO: Added dependency?

        return self.resources_

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Catalog"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Catalog

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["schemas", "is_unity", "grants"]
