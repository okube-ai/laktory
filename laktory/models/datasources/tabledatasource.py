import re
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
    reader_kwargs: dict[str, Any] = Field(
        {},
        description="""
        Keyword arguments passed directly to dataframe backend reader. Passed to `.options()` method when using PySpark.
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

        # names = re.findall(r"\$\{vars\.[^}]+\}|[^.]+", name)
        names = re.split(r"\.(?![^{}]*})", name)

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

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _get_spark_kwargs(self):
        # fmt = self.format.lower()
        fmt = None

        # Build kwargs
        kwargs = {}

        for k, v in self.reader_kwargs.items():
            kwargs[k] = v

        return kwargs, fmt

    def _get_spark_reader_methods(self):
        methods = []

        options, fmt = self._get_spark_kwargs()

        if options:
            methods += [ReaderWriterMethod(name="options", kwargs=options)]

        for m in self.reader_methods:
            methods += [m]

        return methods

    def _read_spark(self) -> nw.LazyFrame:
        from laktory import get_spark_session

        spark = get_spark_session()

        # Create reader
        if self.as_stream:
            mode = "stream"
            reader = spark.readStream
        else:
            mode = "static"
            reader = spark.read

        # Build methods
        methods = self._get_spark_reader_methods()

        # Apply methods
        for m in methods:
            print("Adding method", m.name)
            reader = getattr(reader, m.name)(*m.args, **m.kwargs)

        # Load
        logger.info(
            f"Reading {self._id} as {mode} read.{'.'.join([m.as_string for m in methods])}"
        )
        df = reader.table(self.full_name)

        return nw.from_native(df)
