import json
from typing import Any
from typing import Union

import narwhals as nw
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dtypes import DType

logger = get_logger(__name__)


class ColumnSchema(BaseModel):
    name: str
    dtype: Union[str, DType]
    nullable: bool = True
    is_primary: bool = False

    @field_validator("dtype")
    def set_dtype(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = DType(name=v)
        return v


class DataFrameSchema(BaseModel):
    columns: list[ColumnSchema]

    # Narwhals
    def to_narwhals(self):
        cols = {}
        for c in self.columns:
            cols[c.name] = c.dtype.to_nw
        return nw.Schema(cols)

    # Polars
    def to_polars(self):
        import polars as pl

        cols = {}
        for c in self.columns:
            cols[c.name] = c.dtype.to_polars
        return pl.Schema(cols)

    # Spark
    def to_spark(self):
        import pyspark.sql.types as T

        columns = []
        for c in self.columns:
            _type = c.dtype.to_spark
            columns += [T.StructField(c.name, _type, c.nullable)]

        return T.StructType(columns)

    # String
    def to_string(self, indent=None):
        return json.dumps(
            {c.name: c.dtype.to_string for c in self.columns}, indent=indent
        )
