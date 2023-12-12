from typing import Union
from typing import Literal
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.sql.schema import Schema
from laktory.models.grants.cataloggrant import CatalogGrant


class Catalog(BaseModel, BaseResource):
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
    catalog.deploy()
    ```

    References
    ----------

    * [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#catalogs)
    * [Pulumi Databricks Catalog](https://www.pulumi.com/registry/packages/databricks/api-docs/catalog/)
    """

    comment: Union[str, None] = None
    force_destroy: bool = True
    grants: list[CatalogGrant] = None
    isolation_mode: Literal["OPEN", "ISOLATED"] = "OPEN"
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
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["schemas", "is_unity", "grants"]

    def deploy_with_pulumi(self, name=None, opts=None):
        """
        Deploy catalog using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiCatalog:
            Pulumi catalog resource
        """
        from laktory.resourcesengines.pulumi.catalog import PulumiCatalog

        return PulumiCatalog(name=name, catalog=self, opts=opts)


if __name__ == "__main__":
    from laktory import models

    catalog = models.Catalog(
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
    catalog.deploy()
