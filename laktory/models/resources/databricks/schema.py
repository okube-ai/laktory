from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.grants.schemagrant import SchemaGrant
from laktory.models.resources.databricks.table import Table
from laktory.models.resources.databricks.volume import Volume
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Schema(BaseModel, PulumiResource, TerraformResource):
    """
    A schema (also called a database) is the second layer of Unity Catalog’s
    three-level namespace. A schema organizes tables and views.

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

    catalog_name: Union[str, None] = Field(
        None, description="Name of the catalog storing the schema"
    )
    comment: Union[str, None] = Field(
        None, description="Text description of the catalog"
    )
    force_destroy: bool = Field(
        True, description="If `True` catalog can be deleted, even when not empty"
    )
    grant: Union[SchemaGrant, list[SchemaGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Schema and authoritative for a specific principal. Other principals within the grants are
    preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[SchemaGrant] = Field(
        None,
        description="""
    Grants operating on the Schema and authoritative for all principals. Replaces any existing grants defined inside 
    or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    name: str = Field(..., description="Name of the schema")
    storage_root: str = Field(
        None,
        description="""
    Managed location of the schema. Location in cloud storage where data for managed tables will be stored. If not
    specified, the location will default to the catalog root location.
    """,
    )
    tables: list[Table] = Field([], description="List of tables stored in the schema")
    volumes: list[Volume] = Field(
        [], description="List of volumes stored in the schema"
    )

    @model_validator(mode="after")
    def assign_name(self):
        for table in self.tables:
            table.catalog_name = self.catalog_name
            table.schema_name = self.name
        for volume in self.volumes:
            if self.catalog_name:
                volume.catalog_name = self.catalog_name
            volume.schema_name = self.name

        return self

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
        resources += self.get_grants_additional_resources(
            object={"schema": f"${{resources.{self.resource_name}.id}}"}
        )

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
        return ["tables", "volumes", "grant", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_schema"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
