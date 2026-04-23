from typing import Union

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.grants.tablegrant import TableGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.table_base import TableBase
from laktory.models.resources.pulumiresource import PulumiResource

logger = get_logger(__name__)


class TableLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Full name of the databricks_table: `catalog`.`schema`.`table`",
    )


class Table(TableBase, PulumiResource):
    """
    A table resides in the third layer of Unity Catalog’s three-level namespace. It contains rows of data.

    Examples
    --------
    ```py
    from laktory import models

    table = models.resources.databricks.Table(
        name="slv_stock_prices",
        table_type="MANAGED",
    )
    ```

    References
    ----------

    * [Databricks Unity Table](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables)
    * [Pulumi Databricks Table](https://www.pulumi.com/registry/packages/databricks/api-docs/sqltable/)
    """

    catalog_name: str | None = Field(
        None, description="Name of the catalog storing the table"
    )
    grant: Union[TableGrant, list[TableGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Table and authoritative for a specific principal. Other principals within the grants are
    preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[TableGrant] = Field(
        None,
        description="""
    Grants operating on the Table and authoritative for all principals. Replaces any existing grants defined inside or
    outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    lookup_existing: TableLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    schema_name: str | None = Field(
        None, description="Name of the schema storing the table"
    )

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        """Schema full name `{catalog_name}.{schema_name}`"""
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.schema_name:
            if _id == "":
                _id = self.schema_name
            else:
                _id += f".{self.schema_name}"

        return _id

    @property
    def full_name(self) -> str:
        """Table full name `{catalog_name}.{schema_name}.{table_name}`"""

        if self.lookup_existing:
            return self.lookup_existing.name

        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

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
    def resource_key(self) -> str:
        """Table full name (catalog.schema.table)"""
        return self.full_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - table grants
        """
        resources = []

        # Table grants
        resources += self.get_grants_additional_resources(
            object={"table": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SqlTable"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "grant",
            "grants",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_lookup_type(self) -> str:
        return "databricks_table"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
