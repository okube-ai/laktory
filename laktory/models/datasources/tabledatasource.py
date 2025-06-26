from typing import Any

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.readerwritermethod import ReaderWriterMethod

logger = get_logger(__name__)


class TableDataSource(BaseDataSource):
    catalog_name: str | None = Field(
        None,
        description="Source table catalog name",
    )
    schema_name: str | None = Field(
        None,
        description="Source table schema name",
    )
    table_name: str = Field(
        ...,
        description="""
        Source table name. Also supports fully qualified name (`{catalog}.{schema}.{table}`). In this case, 
        `catalog_name` and `schema_name` arguments are ignored.
        """,
    )
    reader_methods: list[ReaderWriterMethod] = Field(
        [], description="DataFrame backend reader methods."
    )

    @model_validator(mode="after")
    def table_full_name(self) -> Any:
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

    def _read_spark(self, spark=None) -> nw.LazyFrame:
        from laktory import get_spark_session

        spark = get_spark_session()

        if self.as_stream:
            logger.info(f"Reading {self._id} as stream")
            df = spark.readStream.table(self.full_name)
        else:
            logger.info(f"Reading {self._id} as static")
            df = spark.read.table(self.full_name)

        return nw.from_native(df)
