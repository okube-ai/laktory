import os
import shutil
from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.dataframe.dataframeexpr import DataFrameExpr
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.models.datasources.tabledatasource import TableDataSource

logger = get_logger(__name__)


class TableDataSink(BaseDataSink):
    catalog_name: str | None = Field(
        None,
        description="Sink table catalog name",
    )
    format: Literal["PARQUET", "DELTA"] = Field(
        "DELTA", description="Storage format for data table."
    )
    schema_name: str | None = Field(
        None,
        description="Sink table schema name",
    )
    table_name: str = Field(
        ...,
        description="""
        Sink table name. Also supports fully qualified name (`{catalog}.{schema}.{table}`). 
        In this case, `catalog_name` and `schema_name` arguments are ignored.
        """,
    )
    table_type: Literal["TABLE", "VIEW"] = Field(
        "TABLE",
        description="Type of table. 'TABLE' and 'VIEW' are currently supported.",
    )
    view_definition: DataFrameExpr | str = Field(
        None, description="View definition of 'VIEW' `table_type` is selected."
    )

    @model_validator(mode="after")
    def validate_table_full_name(self) -> Any:
        name = self.table_name
        if name is None:
            return
        names = name.split(".")

        with self.validate_assignment_disabled():
            self.table_name = names[-1]
            if len(names) > 1:
                self.schema_name = names[-2]
            if len(names) > 2:
                self.catalog_name = names[-3]

        return self

    @model_validator(mode="after")
    def set_table_type(self):
        with self.validate_assignment_disabled():
            if self.view_definition is not None:
                self.table_type = "VIEW"
        if self.table_type == "VIEW" and self.view_definition is None:
            raise ValueError(
                'View definition must be provided for "VIEW" `table_type`.'
            )
        return self

    @field_validator("view_definition")
    def set_view_definition(
        cls, value: DataFrameExpr | str | None
    ) -> DataFrameExpr | None:
        if value and not isinstance(value, DataFrameExpr):
            value = DataFrameExpr(expr=value)
        return value

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

    @property
    def dlt_name(self) -> str:
        if self.catalog_name:
            # Unity catalog is used only when catalog is defined. In this case
            # DLT allows full name specification
            return self.full_name

        # If catalog is not defined, table is written to Hive Metastore and only table
        # name is allowed
        return self.table_name

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names required to write sink"""
        if self.view_definition:
            return self.view_definition.upstream_node_names
        return []

    @property
    def data_sources(self):
        """Get all sources feeding the sink"""
        if self.view_definition:
            return self.view_definition.data_sources
        return []

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return [
            "view_definition",
        ]

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df, mode, full_refresh=False) -> None:
        df = df.to_native()

        # Full Refresh
        if full_refresh or not self.exists():
            if df.isStreaming:
                pass
            else:
                logger.info(
                    "Full refresh or initial load. Switching to OVERWRITE mode."
                )
                mode = "OVERWRITE"

        # Format
        methods = self._get_spark_writer_methods(mode=mode, is_streaming=df.isStreaming)

        if df.isStreaming:
            logger.info(
                f"Writing df to {self.full_name} with writeStream.{'.'.join([m.as_string for m in methods])}"
            )

            writer = df.writeStream
            for m in methods:
                writer = getattr(writer, m.name)(*m.args, **m.kwargs)

            query = writer.toTable(self.full_name)
            query.awaitTermination()

        else:
            logger.info(
                f"Writing df to {self.full_name} with write.{'.'.join([m.as_string for m in methods])}"
            )
            writer = df.write
            for m in methods:
                writer = getattr(writer, m.name)(*m.args, **m.kwargs)

            writer.saveAsTable(self.full_name)

    def _write_spark_view(self) -> None:
        from laktory import get_spark_session

        spark = get_spark_session()

        logger.info(f"Creating view {self.full_name} AS {self.view_definition.expr}")

        _view = self.view_definition.to_sql()
        df = spark.sql(f"CREATE OR REPLACE VIEW {self.full_name} AS {_view}")
        if self.parent_pipeline_node:
            self.parent_pipeline_node._output_df = df

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def purge(self):
        """
        Delete sink data and checkpoints
        """

        if self.dataframe_backend == DataFrameBackends.PYSPARK:
            from laktory import get_spark_session

            spark = get_spark_session()

            # Remove Data
            logger.info(
                f"Dropping {self.table_type} {self.full_name}",
            )
            spark.sql(f"DROP {self.table_type} IF EXISTS {self.full_name}")

            path = self.writer_kwargs.get("path", None)
            if path:
                path = Path(path)
                if path.exists():
                    is_dir = path.is_dir()
                    if is_dir:
                        logger.info(f"Deleting data dir {path}")
                        shutil.rmtree(path)
                    else:
                        logger.info(f"Deleting data file {path}")
                        os.remove(path)

            # Remove Checkpoint
            self._purge_checkpoint()

        else:
            raise TypeError(
                f"DataFrame backend {self.dataframe_backend} is not supported."
            )

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
            type=self.type,
            dataframe_backend=self.dataframe_backend,
        )

        if as_stream:
            source.as_stream = as_stream

        if self.dataframe_backend_:
            source.dataframe_backend_ = self.dataframe_backend_
        source.parent = self.parent

        return source
