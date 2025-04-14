from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.datasinks.basedatasink import BaseDataSink

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
        description="Sink table name. Also supports fully qualified name (`{catalog}.{schema}.{table}`). In this case, `catalog_name` and `schema_name` arguments are ignored.",
    )
    # table_type: Literal["TABLE", "VIEW"] = Field(
    #     "TABLE", description="Type of table. 'TABLE' and 'VIEW' are currently supported."
    # )
    # view_definition: str = Field(
    #     None,
    #     description="View definition of 'VIEW' `table_type` is selected."
    # )
    # _parsed_view_definition: BaseChainNodeSQLExpr = None
    _parsed_view_definition: str = None

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

    # @model_validator(mode="after")
    # def set_table_type(self):
    #     with self.validate_assignment_disabled():
    #         if self.view_definition is not None:
    #             self.table_type = "VIEW"
    #     return self

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

    # # ----------------------------------------------------------------------- #
    # # View Definition                                                         #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def parsed_view_definition(self):
    #     if self.view_definition is None:
    #         return None
    #     if not self._parsed_view_definition:
    #         self._parsed_view_definition = BaseChainNodeSQLExpr(
    #             expr=self.view_definition
    #         )
    #     return self._parsed_view_definition

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df, mode, full_refresh=False) -> None:
        df = df.to_native()

        # TODO: Add Support for VIEW

        # Full Refresh
        # if full_refresh or not self.exists(spark=df.sparkSession):
        #     if df.isStreaming:
        #         pass
        #         # .is_aggregate() method seems unreliable. Disabling for now.
        #         # if df.laktory.is_aggregate():
        #         #     logger.info(
        #         #         "Full refresh or initial load. Switching to COMPLETE mode."
        #         #     )
        #         #     mode = "COMPLETE"
        #     else:
        #         logger.info(
        #             "Full refresh or initial load. Switching to OVERWRITE mode."
        #         )
        #         mode = "OVERWRITE"

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

    # # ----------------------------------------------------------------------- #
    # # Purge                                                                   #
    # # ----------------------------------------------------------------------- #
    #
    # def purge(self, spark=None):
    #     """
    #     Delete sink data and checkpoints
    #     """
    #     # TODO: Now that sink switch to overwrite when sink does not exists or when
    #     # a full refresh is requested, the purge method should not delete the data
    #     # by default, but only the checkpoints. Also consider truncating the table
    #     # instead of dropping it.
    #
    #     # Remove Data
    #     if self.warehouse == "DATABRICKS":
    #         logger.info(
    #             f"Dropping {self.table_type} {self.full_name}",
    #         )
    #         spark.sql(f"DROP {self.table_type} IF EXISTS {self.full_name}")
    #
    #         path = self.write_options.get("path", None)
    #         if path and os.path.exists(path):
    #             is_dir = os.path.isdir(path)
    #             if is_dir:
    #                 logger.info(f"Deleting data dir {path}")
    #                 shutil.rmtree(path)
    #             else:
    #                 logger.info(f"Deleting data file {path}")
    #                 os.remove(path)
    #     else:
    #         raise NotImplementedError(
    #             f"Warehouse '{self.warehouse}' is not yet supported."
    #         )
    #
    #     # Remove Checkpoint
    #     self._purge_checkpoint(spark=spark)
    #
    # # ----------------------------------------------------------------------- #
    # # Source                                                                  #
    # # ----------------------------------------------------------------------- #
    #
    # def as_source(self, as_stream=None) -> TableDataSource:
    #     """
    #     Generate a table data source with the same properties as the sink.
    #
    #     Parameters
    #     ----------
    #     as_stream:
    #         If `True`, sink will be read as stream.
    #
    #     Returns
    #     -------
    #     :
    #         Table Data Source
    #     """
    #     source = TableDataSource(
    #         catalog_name=self.catalog_name,
    #         table_name=self.table_name,
    #         schema_name=self.schema_name,
    #         warehouse=self.warehouse,
    #     )
    #
    #     if as_stream:
    #         source.as_stream = as_stream
    #
    #     if self.dataframe_backend:
    #         source.dataframe_backend = self.dataframe_backend
    #     source.parent = self.parent
    #
    #     return source
