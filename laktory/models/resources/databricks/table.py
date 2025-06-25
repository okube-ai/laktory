from typing import Literal
from typing import Union

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.grants.tablegrant import TableGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

logger = get_logger(__name__)


class TableLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Full name of the databricks_table: `catalog`.`schema`.`table`",
    )


class TableColumn(BaseModel):
    name: str = Field(..., description="User-visible name of column")
    comment: str = Field(None, description="User-supplied free-form text.")
    identity: str = Field(
        None,
        description="Whether field is an identity column. Can be `default`, `always` or `unset`. It is `unset` by default.",
    )
    nullable: bool = Field(
        None, description="Whether field is nullable (Default: `true`)"
    )
    type: str = Field(
        None,
        description="Column type spec (with metadata) as SQL text. Not supported for `VIEW` table_type.",
    )
    type_json: str = Field(None, description="")


class Table(BaseModel, PulumiResource, TerraformResource):
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

    catalog_name: Union[str, None] = Field(
        None, description="Name of the catalog storing the table"
    )
    columns: Union[list[TableColumn], None] = Field(
        None, description="List of columns stored in the table"
    )
    comment: Union[str, None] = Field(
        None, description="Text description of the catalog"
    )
    data_source_format: str = Field(
        "DELTA",
        description="""
    External tables are supported in multiple data source formats. The string constants identifying these formats
    are DELTA, CSV, JSON, AVRO, PARQUET, ORC, TEXT. Change forces creation of a new resource. Not supported for
    MANAGED tables or VIEW.
    """,
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
    name: str = Field(..., description="Name of the table")
    properties: Union[dict[str, str], None] = Field(None, description="")
    schema_name: Union[str, None] = Field(
        None, description="Name of the schema storing the table"
    )
    storage_credential_name: Union[str, None] = Field(
        None,
        description="For EXTERNAL Tables only: the name of storage credential to use. Change forces creation of a new resource.",
    )
    storage_location: Union[str, None] = Field(
        None,
        description="URL of storage location for Table data (required for EXTERNAL Tables). Not supported for VIEW or MANAGED table_type.",
    )
    table_type: Literal["MANAGED", "EXTERNAL", "VIEW"] = Field(
        "MANAGED", description="Distinguishes a view vs. managed/external Table."
    )  # required
    view_definition: Union[str, None] = Field(
        None,
        description="SQL text defining the view (for `table_type == 'VIEW'`). Not supported for MANAGED or EXTERNAL table_type.",
    )
    warehouse_id: Union[str, None] = Field(
        None,
        description="""
    All table CRUD operations must be executed on a running cluster or SQL warehouse. If a warehouse_id is specified, 
    that SQL warehouse will be used to execute SQL commands to manage this table.
    """,
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
        return [c.name for c in self.columns]

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
    def singularizations(self) -> dict[str, str]:
        return {
            "columns": "column",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_table"

    @property
    def terraform_resource_lookup_type(self) -> str:
        return "databricks_table"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
