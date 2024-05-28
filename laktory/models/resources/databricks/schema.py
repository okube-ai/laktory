from typing import Union

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.grants import Grants
from laktory.models.grants.schemagrant import SchemaGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.table import Table
from laktory.models.resources.databricks.volume import Volume


class Schema(BaseModel, PulumiResource, TerraformResource):
    """
    A schema (also called a database) is the second layer of Unity Catalog’s
    three-level namespace. A schema organizes tables and views.

    Attributes
    ----------
    catalog_name:
        Name of the catalog storing the schema
    comment:
        Text description of the catalog
    force_destroy:
        If `True` catalog can be deleted, even when not empty
    grants:
        List of grants operating on the schema
    isolation_mode:
        Whether the catalog is accessible from all workspaces or a specific set
        of workspaces. Can be ISOLATED or OPEN. Setting the catalog to ISOLATED
        will automatically allow access from the current workspace.
    name:
        Name of the catalog
    tables:
        List of tables stored in the schema
    volumes:
        List of volumes stored in the schema

    Examples
    --------
    ```py
    from laktory import models

    schema = models.resources.databricks.Schema(
        catalog_name="dev",
        name="engineering",
        grants=[{"principal": "domain-engineering", "privileges": ["SELECT"]}],
    )
    ```

    References
    ----------

    * [Databricks Unity Schema](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#schemas)
    * [Pulumi Databricks Schema](https://www.pulumi.com/registry/packages/databricks/api-docs/schema/)
    """

    catalog_name: Union[str, None] = None
    comment: Union[str, None] = None
    force_destroy: bool = True
    grants: list[SchemaGrant] = None
    name: str
    tables: list[Table] = []
    volumes: list[Volume] = []

    def model_post_init(self, __context):
        super().model_post_init(__context)
        for table in self.tables:
            table.catalog_name = self.catalog_name
            table.schema_name = self.name
        for volume in self.volumes:
            volume.catalog_name = self.catalog_name
            volume.schema_name = self.name

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        """Catalog full name `{catalog_name}`"""
        return self.catalog_name

    @property
    def full_name(self) -> str:
        """Schema full name `{catalog_name}.{schema_name}`"""
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Schema full name (catalog.schema)"""
        return self.full_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - schema grants
        - tables
        - volumes
        """
        resources = []

        # Schema grants
        if self.grants:
            resources += [
                Grants(
                    resource_name=f"grants-{self.resource_name}",
                    schema=self.full_name,
                    grants=[
                        {"principal": g.principal, "privileges": g.privileges}
                        for g in self.grants
                    ],
                )
            ]

        if self.volumes:
            for v in self.volumes:
                resources += v.core_resources

        if self.tables:
            for t in self.tables:
                resources += t.core_resources

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Schema"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["tables", "volumes", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_schema"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
