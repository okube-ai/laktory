import json
from typing import Any
from typing import Union

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframecolumn import DataFrameColumn
from laktory.models.dtypes import DType

logger = get_logger(__name__)


class DataFrameSchema(BaseModel):
    """
    DataFrame schema. Typically used to explicitly express a schema when reading files.

    Examples
    --------
    ```python
    from laktory import models

    schema = models.DataFrameSchema(columns={"a": "string", "x": "double"})
    ```
    """

    columns: Union[
        dict[str, Union[str, DType, DataFrameColumn]], list[DataFrameColumn]
    ] = Field(..., description="Dict or list of columns")

    @model_validator(mode="before")
    @classmethod
    def set_columns(cls, data: Any) -> Any:
        if "columns" not in data:
            return data

        columns = data["columns"]

        if isinstance(columns, dict):
            for k, v in columns.items():
                if isinstance(v, (str, DType)):
                    v = {"dtype": v}

                if isinstance(v, DataFrameColumn):
                    v.name = k
                else:
                    v["name"] = k

                columns[k] = v

            data["columns"] = list(columns.values())

        return data

    # -------------------------------------------------------------------------------- #
    # Class Methods                                                                    #
    # -------------------------------------------------------------------------------- #

    # From Narwhals
    @classmethod
    def from_narwhals(cls, schema: nw.Schema) -> "DataFrameSchema":
        """Create a DataFrameSchema from a Narwhals schema"""
        columns = [
            DataFrameColumn(name=name, dtype=DType.from_narwhals(dtype))
            for name, dtype in schema.items()
        ]
        return cls(columns=columns)

    # -------------------------------------------------------------------------------- #
    # Instance Methods                                                                 #
    # -------------------------------------------------------------------------------- #

    def to_narwhals(self) -> nw.Schema:
        """Returns a Narwhals schema object"""
        cols = {}
        for c in self.columns:
            cols[c.name] = c.dtype.to_narwhals()
        return nw.Schema(cols)

    def to_native(
        self, dataframe_backend: nw.Implementation | DataFrameBackends | None = None
    ):
        dataframe_backend = DataFrameBackends.from_any(dataframe_backend)

        if dataframe_backend == DataFrameBackends.PYSPARK:
            return self.to_pyspark()
        elif dataframe_backend == DataFrameBackends.POLARS:
            return self.to_polars()
        else:
            raise NotImplementedError()

    # Polars
    def to_polars(self):
        """Returns a Polars schema object"""
        import polars as pl

        cols = {}
        for c in self.columns:
            cols[c.name] = c.dtype.to_polars()
        return pl.Schema(cols)

    # Spark
    def to_pyspark(self):
        """Returns a Spark schema object"""
        import pyspark.sql.types as T

        columns = []
        for c in self.columns:
            _type = c.dtype.to_pyspark()
            columns += [T.StructField(c.name, _type, c.nullable)]

        return T.StructType(columns)

    # -------------------------------------------------------------------------------- #
    # Outputs                                                                          #
    # -------------------------------------------------------------------------------- #

    # String
    def to_string(self, indent=None):
        """Returns a string representation of the schema"""
        return json.dumps(
            {c.name: c.dtype.to_string() for c in self.columns}, indent=indent
        )
