import json
from typing import Any
from typing import Union

import narwhals as nw
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dataframecolumn import DataFrameColumn
from laktory.models.dtypes import DType

logger = get_logger(__name__)


class DataFrameSchema(BaseModel):
    columns: Union[dict[str, Union[str, DType, DataFrameColumn]], list[DataFrameColumn]]

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

    # Narwhals
    def to_narwhals(self):
        cols = {}
        for c in self.columns:
            cols[c.name] = c.dtype.to_narwhals()
        return nw.Schema(cols)

    # Polars
    def to_polars(self):
        import polars as pl

        cols = {}
        for c in self.columns:
            cols[c.name] = c.dtype.to_polars()
        return pl.Schema(cols)

    # Spark
    def to_spark(self):
        import pyspark.sql.types as T

        columns = []
        for c in self.columns:
            _type = c.dtype.to_spark()
            columns += [T.StructField(c.name, _type, c.nullable)]

        return T.StructType(columns)

    # String
    def to_string(self, indent=None):
        return json.dumps(
            {c.name: c.dtype.to_string() for c in self.columns}, indent=indent
        )
