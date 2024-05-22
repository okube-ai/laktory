from typing import Any
from typing import Union
from typing import Literal

from pydantic import model_validator

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.grants import Grants
from laktory.models.grants.tablegrant import TableGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.column import Column
from laktory.models.resources.databricks.tablebuilder import TableBuilder
from laktory.models.resources.databricks.tableexpectation import TableExpectation
from laktory.models.datasources.tabledatasource import TableDataSource

logger = get_logger(__name__)


class Table(BaseModel, PulumiResource, TerraformResource):
    """
    A table resides in the third layer of Unity Catalog’s three-level
    namespace. It contains rows of data. Laktory provides the mechanism to
    build the table data in the context of a data pipeline using the
    `builder` attribute.

    Attributes
    ----------
    builder:
        Instructions on how to build data from a source in the context of a
        data pipeline.
    catalog_name:
        Name of the catalog storing the table
    columns:
        List of columns stored in the table
    comment:
        Text description of the catalog
    data:
        Data to be used to populate the rows
    expectations:
        List of expectations for the table. Can be used as warnings, drop
        invalid records or fail a pipeline.
    grants:
        List of grants operating on the schema
    name:
        Name of the table
    primary_key:
        Name of the column storing a unique identifier for each row. It is used
        by the builder to drop duplicated rows.
    schema_name:
        Name of the schema storing the table
    table_type:
        Distinguishes a view vs. managed/external Table.
    timestamp_key:
        Name of the column storing a timestamp associated with each row. It is
        used as the default column by the builder when creating watermarks.
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
    from laktory import dlt
    from laktory import models

    dlt.spark = spark

    table = models.resources.databricks.Table(
        name="slv_stock_prices",
        builder={
            "layer": "SILVER",
            "source": {
                "table_name": "brz_stock_prices",
            },
            "spark_chain": {
                "nodes": [
                    {
                        "column": {"name": "symbol", "type": "string"},
                        "sql_expression": "data.symbol",
                    },
                    {
                        "column": {
                            "name": "open",
                            "type": "double",
                        },
                        "spark_func_name": "coalesce",
                        "spark_func_args": ["daa.open"],
                    },
                    {
                        "column": {
                            "name": "close",
                            "type": "double",
                        },
                        "spark_func_name": "coalesce",
                        "spark_func_args": ["daa.close"],
                    },
                ]
            },
        },
    )

    # Read
    # df = table.builder.read_source(spark)

    # Process
    # df = table.builder.process(df, None)
    ```

    References
    ----------

    * [Databricks Unity Table](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#tables)
    * [Pulumi Databricks Table](https://www.pulumi.com/registry/packages/databricks/api-docs/sqltable/)
    """

    builder: TableBuilder = TableBuilder()
    catalog_name: Union[str, None] = None
    columns: list[Column] = []
    comment: Union[str, None] = None
    data: list[list[Any]] = None
    data_source_format: str = "DELTA"
    expectations: list[TableExpectation] = []
    grants: list[TableGrant] = None
    name: str
    primary_key: Union[str, None] = None
    schema_name: Union[str, None] = None
    table_type: Literal["MANAGED", "EXTERNA", "VIEW"] = "MANAGED"
    timestamp_key: Union[str, None] = None
    view_definition: str = None
    warehouse_id: str = None

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    @model_validator(mode="after")
    def assign_catalog_schema(self) -> Any:
        # Assign to columns
        for c in self.columns:
            c.table_name = self.name
            c.catalog_name = self.catalog_name
            c.schema_name = self.schema_name

        # Set builder table
        self.builder._table = self

        # Find table sources
        sources = []
        if isinstance(self.builder.source, TableDataSource):
            sources += [self.builder.source]

        if self.builder.spark_chain:
            for n in self.builder.spark_chain.nodes:
                for a in n.spark_func_args:
                    if isinstance(a.value, TableDataSource):
                        sources += [a.value]
                for a in n.spark_func_kwargs.values():
                    if isinstance(a.value, TableDataSource):
                        sources += [a.value]

        # Assign catalog and schema to sources
        for s in sources:
            if s.catalog_name is None:
                s.catalog_name = self.catalog_name
            if s.schema_name is None:
                s.schema_name = self.schema_name

        # Warehouse ID
        if self.warehouse_id is None:
            self.warehouse_id = settings.databricks_warehouse_id

        return self

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
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    @property
    def layer(self) -> str:
        """Layer in the medallion architecture ("BRONZE", "SILVER", "GOLD")"""
        return self.builder.layer

    @property
    def column_names(self) -> list[str]:
        """List of column names"""
        return [c.name for c in self.columns]

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        return self.builder.is_from_cdc

    @property
    def warning_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.action == "WARN":
                expectations[e.name] = e.expression
        return expectations

    @property
    def drop_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.action == "DROP":
                expectations[e.name] = e.expression
        return expectations

    @property
    def fail_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.action == "FAIL":
                expectations[e.name] = e.expression
        return expectations

    # ----------------------------------------------------------------------- #
    #  Methods                                                                #
    # ----------------------------------------------------------------------- #

    def to_df(self, spark=None):
        """
        Dataframe representation of the table. Requires `self.data` to be
        specified.

        Attributes
        ----------
        spark: SparkSession
            Spark session used to convert pandas DataFrame into a spark
            DataFrame if provided.
        """
        import pandas as pd

        df = pd.DataFrame(data=self.data, columns=self.column_names)

        if spark:
            df = spark.createDataFrame(df)
        return df

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Table full name (catalog.schema.table)"""
        return self.full_name

    @property
    def self_as_core_resources(self):
        return not self.builder.pipeline_name

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
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.SqlTable

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "builder",
            "columns",
            "data",
            "expectations",
            "grants",
            "primary_key",
            "timestamp_key",
        ]

    @property
    def pulumi_properties(self):
        d = super().pulumi_properties
        d["columns"] = []
        for i, c in enumerate(self.columns):
            d["columns"] += [
                {
                    "name": c.name,
                    "comment": c.comment,
                    "type": c.type,
                }
            ]
        return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_table"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties
        d["column"] = []
        for i, c in enumerate(self.columns):
            d["column"] += [
                {
                    "name": c.name,
                    "comment": c.comment,
                    "type": c.type,
                }
            ]
        return d