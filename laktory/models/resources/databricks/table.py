from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import PluralField
from laktory.models.grants.tablegrant import TableGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks._accessrequestdestinationsmixin import (
    AccessRequestDestinationsMixin,
)
from laktory.models.resources.databricks._policiesmixin import PoliciesMixin
from laktory.models.resources.databricks._tagsmixin import TagsMixin
from laktory.models.resources.databricks._unitycatalogmixin import UnityCatalogMixin
from laktory.models.resources.databricks.table_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.table_base import TableBase
from laktory.models.resources.databricks.table_base import TableColumn as _TableColumn

logger = get_logger(__name__)


class TableLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Full name of the databricks_table: `catalog`.`schema`.`table`",
    )


class TableColumn(_TableColumn):
    tags: dict[str, str | None] = Field(
        None,
        description="Tags assigned to the column as `{tag_key: tag_value}`. Use a `null` value for a key-only tag. Each tag is deployed as a separate tag assignment resource.",
    )


class Table(
    UnityCatalogMixin,
    TagsMixin,
    PoliciesMixin,
    AccessRequestDestinationsMixin,
    TableBase,
):
    """
    A table resides in the third layer of Unity Catalog’s three-level namespace. It contains rows of data.

    Examples
    --------
    ```py
    import io

    from laktory import models

    table_yaml = '''
    name: slv_stock_prices
    catalog_name: dev
    schema_name: finance
    table_type: MANAGED
    columns:
    - name: symbol
      type: STRING
    - name: trader_email
      type: STRING
      tags:
        pii: email
    tags:
      data_classification: confidential
    grants:
    - principal: account users
      privileges:
      - SELECT
    '''
    table = models.resources.databricks.Table.model_validate_yaml(
        io.StringIO(table_yaml)
    )
    ```

    References
    ----------

    * [Databricks Unity Table](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables)
    """

    __optional_fields__ = ["catalog_name", "schema_name"]
    grant: TableGrant | list[TableGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[TableGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Table - including those set
    outside Laktory - with only the entries listed here. Use only when Laktory owns all access management for this
    resource. Mutually exclusive with `grant`.
    """,
    )
    column: list[TableColumn] | None = PluralField(
        None, plural="columns", description="Table columns"
    )
    lookup_existing: TableLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing Table by full `name` (`catalog.schema.table`) instead of creating it. The table becomes available for cross-referencing and child resource deployment (grants, etc.); its own field values are not written to the existing resource.",
    )
    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Table full name `{catalog_name}.{schema_name}.{table_name}`"""
        if self.lookup_existing:
            return self.lookup_existing.name
        return super().full_name

    @property
    def column_names(self) -> list[str]:
        """List of column names"""
        return [c.name for c in self.column]

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        return self.builder.is_from_cdc

    # ----------------------------------------------------------------------- #
    #  Methods                                                                #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - table grants
        - table and columns tags
        - table policies
        - table access request destinations
        """
        resources = []

        # Table grants
        resources += self.get_grants_additional_resources(
            object={"table": f"${{resources.{self.resource_name}.id}}"}
        )

        # Table and columns tags
        table_id = f"${{resources.{self.resource_name}.id}}"
        resources += self.get_tags_additional_resources("tables", table_id)
        for c in self.column or []:
            resources += self.get_tags_additional_resources(
                "columns", f"{table_id}.{c.name}", tags=c.tags or {}
            )

        # Table policies
        resources += self.get_policies_additional_resources("TABLE")

        # Table access request destinations
        resources += self.get_access_request_destinations_additional_resources("TABLE")
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_lookup_type(self) -> str:
        return "databricks_table"

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return {
            "grant": True,
            "grants": True,
            "tags": True,
            "policies": True,
            "access_request_destinations": True,
            "column": {"__all__": {"tags"}},
        }
