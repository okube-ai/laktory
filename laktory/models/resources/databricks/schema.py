from pydantic import Field
from pydantic import model_validator

from laktory.models.grants.schemagrant import SchemaGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks._unitycatalogmixin import UnityCatalogMixin
from laktory.models.resources.databricks.schema_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.schema_base import SchemaBase
from laktory.models.resources.databricks.table import Table
from laktory.models.resources.databricks.volume import Volume


class SchemaLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Full name of the schema: `catalog`.`schema`",
    )


class Schema(UnityCatalogMixin, SchemaBase):
    """
    A schema (also called a database) is the second layer of Unity Catalog's
    three-level namespace. A schema organizes tables and views.

    Examples
    --------
    ```py
    import io

    from laktory import models

    schema_yaml = '''
    catalog_name: dev
    name: engineering
    grants:
    - principal: domain-engineering
      privileges:
      - SELECT
    '''
    schema = models.resources.databricks.Schema.model_validate_yaml(
        io.StringIO(schema_yaml)
    )
    ```

    References
    ----------

    * [Databricks Unity Schema](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#schemas)
    """

    # Relax fields the base marks required but Laktory fills via parent validators
    catalog_name: str = Field(
        None, description="Name of the catalog storing the schema"
    )
    lookup_existing: SchemaLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing Schema by full `name` (`catalog.schema`) instead of creating it. The schema becomes available for cross-referencing and child resource deployment (grants, tables, volumes, etc.); its own field values are not written to the existing resource.",
    )

    # Override base default (None → True)
    force_destroy: bool = Field(
        True, description="If `True` schema can be deleted, even when not empty"
    )

    # Laktory-specific
    grant: SchemaGrant | list[SchemaGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[SchemaGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Schema — including those set
    outside Laktory — with only the entries listed here. Use only when Laktory owns all access management for this
    resource. Mutually exclusive with `grant`.
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
    def full_name(self) -> str:
        """Full schema name `{catalog_name}.{schema_name}`"""
        if self.lookup_existing:
            return self.lookup_existing.name
        return super().full_name

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["tables", "volumes", "grant", "grants"]
