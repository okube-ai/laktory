from typing import Union
from typing import Literal
from pydantic import Field
from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.grants import Grants
from laktory.models.grants.tablegrant import TableGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

logger = get_logger(__name__)


class TableLookup(ResourceLookup):
    """
    Attributes
    ----------
    name:
        Full name of the databricks_table: `catalog`.`schema`.`table`
    """

    name: str = Field(serialization_alias="id")


class TableColumn(BaseModel):
    """
    A table column.

    Attributes
    ----------
    name:
        User-visible name of column
    comment:
        User-supplied free-form text.
    identity:
        Whether field is an identity column. Can be `default`, `always` or
        `unset`. It is `unset` by default.
    nullable:
        Whether field is nullable (Default: `true`)
    type:
        Column type spec (with metadata) as SQL text. Not supported for `VIEW`
        table_type.
    type_json:

    """

    name: str
    comment: str = None
    identity: str = None
    nullable: bool = None
    type: str = None
    type_json: str = None


class Table(BaseModel, PulumiResource, TerraformResource):
    """
    A table resides in the third layer of Unity Catalog’s three-level
    namespace. It contains rows of data.

    Attributes
    ----------
    catalog_name:
        Name of the catalog storing the table
    columns:
        List of columns stored in the table
    comment:
        Text description of the catalog
    data_source_format:
        External tables are supported in multiple data source formats. The string constants identifying these formats
        are DELTA, CSV, JSON, AVRO, PARQUET, ORC, TEXT. Change forces creation of a new resource. Not supported for
        MANAGED tables or VIEW.
    grants:
        List of grants operating on the schema
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    name:
        Name of the table
    schema_name:
        Name of the schema storing the table
    storage_credential_name:
        For EXTERNAL Tables only: the name of storage credential to use. Change forces creation of a new resource.
    storage_location:
        URL of storage location for Table data (required for EXTERNAL Tables). Not supported for VIEW or MANAGED
        table_type.
    table_type:
        Distinguishes a view vs. managed/external Table.
    view_definition:
        SQL text defining the view (for `table_type == "VIEW"`). Not supported
        for MANAGED or EXTERNAL table_type.
    warehouse_id:
        All table CRUD operations must be executed on a running cluster or SQL
        warehouse. If a warehouse_id is specified, that SQL warehouse will be
        used to execute SQL commands to manage this table.

    Examples
    --------
    ```py
    from laktory import models

    table = models.resources.databricks.Table(
        name="slv_stock_prices",
    )
    ```

    References
    ----------

    * [Databricks Unity Table](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables)
    * [Pulumi Databricks Table](https://www.pulumi.com/registry/packages/databricks/api-docs/sqltable/)
    """

    catalog_name: Union[str, None] = None
    columns: Union[list[TableColumn], None] = None
    comment: Union[str, None] = None
    data_source_format: str = "DELTA"
    grants: list[TableGrant] = None
    lookup_existing: TableLookup = Field(None, exclude=True)
    name: str
    properties: Union[dict[str, str], None] = None
    schema_name: Union[str, None] = None
    storage_credential_name: Union[str, None] = None
    storage_location: Union[str, None] = None
    table_type: Literal["MANAGED", "EXTERNAL", "VIEW"] = "MANAGED"
    view_definition: Union[str, None] = None
    warehouse_id: Union[str, None] = None

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

        # Schema grants
        if self.grants:
            resources += [
                Grants(
                    resource_name=f"grants-{self.resource_name}",
                    table=self.full_name,
                    grants=[
                        {"principal": g.principal, "privileges": g.privileges}
                        for g in self.grants
                    ],
                )
            ]
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
