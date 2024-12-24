import os
import shutil
from typing import Literal
from typing import Union
from pydantic import model_validator
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.spark import SparkDataFrame
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.models.transformers.basechainnode import BaseChainNodeSQLExpr
from laktory._logger import get_logger

logger = get_logger(__name__)


class TableDataSink(BaseDataSink):
    """
    Table data sink on a metastore such as Hive, Unity Catalog or on a
    data warehouse such as Snowflake, BigQuery, etc.

    Attributes
    ----------
    checkpoint_location:
        Path to which the checkpoint file for streaming dataframe should
        be written.
    catalog_name:
        Name of the catalog of the sink table
    table_name:
        Name of the sink table
    table_type:
        Type of table. "TABLE" and "VIEW" are currently supported.
    schema_name:
        Name of the schema of the source table
    view_definition:
        View definition of "VIEW" `table_type` is selected.
    warehouse:
        Type of warehouse to which the table should be published

    Examples
    ---------
    ```python
    from laktory import models
    import pandas as pd

    df = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "price": [200.0, 205.0],
                "tstamp": ["2023-09-01", "2023-09-01"],
            }
        )
    )

    sink = models.TableDataSink(
        catalog_name="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        schema_name="finance",
        table_name="slv_stock_prices",
        mode="OVERWRITE",
    )
    # sink.write(df)

    # Sink with Change Data Capture processing
    sink = models.TableDataSink(
        catalog_name="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        schema_name="finance",
        table_name="slv_stock_prices",
        mode="MERGE",
        merge_cdc_options={
            "scd_type": 1,
            "primary_keys": ["symbol", "tstamp"],
        },
    )
    # sink.write(df)
    ```
    """

    catalog_name: Union[str, None] = None
    checkpoint_location: Union[str, None] = None
    format: Literal["DELTA", "PARQUET"] = "DELTA"
    schema_name: Union[str, None] = None
    table_name: Union[str, None]
    table_type: Literal["TABLE", "VIEW"] = "TABLE"
    view_definition: str = None
    warehouse: Union[Literal["DATABRICKS"], None] = "DATABRICKS"
    _parsed_view_definition: BaseChainNodeSQLExpr = None

    @model_validator(mode="after")
    def set_table_type(self):
        if self.view_definition is not None:
            self.table_type = "VIEW"
        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Table full name {catalog_name}.{schema_name}.{table_name}"""
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
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def child_attribute_names(self):
        return [
            "_parsed_view_definition",
        ]

    # ----------------------------------------------------------------------- #
    # View Definition                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parsed_view_definition(self):
        if self.view_definition is None:
            return None
        if not self._parsed_view_definition:
            self._parsed_view_definition = BaseChainNodeSQLExpr(
                expr=self.view_definition
            )
        return self._parsed_view_definition

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df: SparkDataFrame, mode=None) -> None:

        if df.isStreaming and self._checkpoint_location is None:
            raise ValueError("Checkpoint must be provided for streaming table sink.")

        if mode is None:
            mode = self.mode

        if self.warehouse == "DATABRICKS":
            return self._write_spark_databricks(df, mode=mode)
        else:
            raise NotImplementedError(
                f"Warehouse '{self.warehouse}' is not yet supported."
            )

    def _write_spark_view(self, view_definition: str, spark) -> None:
        logger.info(f"Creating view {self.full_name} AS {view_definition}")
        df = spark.sql(f"CREATE OR REPLACE VIEW {self.full_name} AS {view_definition}")
        if self.parent_pipeline_node:
            self.parent_pipeline_node._output_df = df

    def _write_spark_databricks(self, df: SparkDataFrame, mode) -> None:

        if self.format in ["EXCEL"]:
            raise ValueError(f"'{self.format}' format is not supported with Spark")

        if mode.lower() == "merge":
            self.merge_cdc_options.execute(source=df)
            return

        # Default Options
        _options = {"mergeSchema": "true", "overwriteSchema": "false"}
        if mode in ["OVERWRITE", "COMPLETE"]:
            _options["mergeSchema"] = "false"
            _options["overwriteSchema"] = "true"
        if df.isStreaming:
            _options["checkpointLocation"] = self._checkpoint_location

        # User Options
        for k, v in self.write_options.items():
            _options[k] = v

        if df.isStreaming:

            logger.info(
                f"Writing {self._id} {self.format}  as stream with mode {mode} and options {_options}"
            )
            query = (
                df.writeStream.outputMode(mode)
                .format(self.format.lower())
                .trigger(availableNow=True)  # TODO: Add option for trigger?
                .options(**_options)
            ).toTable(self.full_name)

            query.awaitTermination()

        else:
            logger.info(
                f"Writing {self._id} {self.format}  as static with mode {mode} and options {_options}"
            )
            (
                df.write.format(self.format.lower())
                .mode(mode)
                .options(**_options)
                .saveAsTable(self.full_name)
            )

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def purge(self, spark=None):
        """
        Delete sink data and checkpoints
        """
        # Remove Data
        if self.warehouse == "DATABRICKS":

            logger.info(
                f"Dropping {self.table_type} {self.full_name}",
            )
            spark.sql(f"DROP {self.table_type} IF EXISTS {self.full_name}")

            path = self.write_options.get("path", None)
            if path and os.path.exists(path):
                is_dir = os.path.isdir(path)
                if is_dir:
                    logger.info(f"Deleting data dir {path}")
                    shutil.rmtree(path)
                else:
                    logger.info(f"Deleting data file {path}")
                    os.remove(path)
        else:
            raise NotImplementedError(
                f"Warehouse '{self.warehouse}' is not yet supported."
            )

        # Remove Checkpoint
        self._purge_checkpoint(spark=spark)

    # ----------------------------------------------------------------------- #
    # Source                                                                  #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream=None) -> TableDataSource:
        """
        Generate a table data source with the same properties as the sink.

        Parameters
        ----------
        as_stream:
            If `True`, sink will be read as stream.

        Returns
        -------
        :
            Table Data Source
        """
        source = TableDataSource(
            catalog_name=self.catalog_name,
            table_name=self.table_name,
            schema_name=self.schema_name,
            warehouse=self.warehouse,
        )

        if as_stream:
            source.as_stream = as_stream

        source.parent = self.parent

        return source
