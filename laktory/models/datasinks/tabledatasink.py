from typing import Literal
from typing import Union
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.spark import SparkDataFrame


class TableDataSink(BaseDataSink):
    """
    Data Table data sink such as table on a Databricks catalog or on a
    data warehouse such as Snowflake, BigQuery, etc.

    Attributes
    ----------
    catalog_name:
        Name of the catalog of the source table
    table_name:
        Name of the source table
    schema_name:
        Name of the schema of the source table
    warehouse:
        Type of warehouse to which the table should be published

    Examples
    ---------
    ```python
    from laktory import models

    sink = models.TableDataSink(
        catalog_name="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        schema_name="finance",
        table_name="slv_stock_prices",
        mode="overwrite",
        as_stream=False,
    )
    sink.write(df)
    ```
    """
    catalog_name: Union[str, None] = None
    format: Literal["DELTA", "PARQUET"] = "DELTA"
    schema_name: Union[str, None] = None
    table_name: Union[str, None]
    warehouse: Union[Literal["DATABRICKS"], None] = "DATABRICKS"

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        if self.table_name is None:
            return None

        name = ""
        if self.catalog_name is not None:
            name = self.catalog_name

        if self.schema_name is not None:
            if name == "":
                name = self.schema_name
            else:
                name += f".{self.schema_name}"

        if name == "":
            name = self.table_name
        else:
            name += f".{self.table_name}"

        return name

    @property
    def _id(self) -> str:
        return self.full_name

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df: SparkDataFrame, mode=None) -> None:

        if mode is None:
            mode = self.mode

        if self.warehouse == "DATABRICKS":
            return self._write_spark_databricks(df, mode=mode)
        else:
            raise NotImplementedError(
                f"Warehouse '{self.warehouse}' is not yet supported."
            )

    def _write_spark_databricks(self, df: SparkDataFrame, mode) -> None:
        (df.write.format(self.format).mode(mode).saveAsTable(self.full_name))
