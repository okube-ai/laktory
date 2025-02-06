import json

import narwhals as nw

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True
    is_primary: bool = False

    @property
    def nw_dtype(self):
        dtypes = nw.dtypes
        _type = self.type.lower()

        # Integer types
        if _type in ["decimal"]:
            return dtypes.Decimal
        elif _type in ["int128"]:
            return dtypes.Int128
        elif _type in ["int64", "bigint"]:
            return dtypes.Int64
        elif _type in ["int32", "int", "integer"]:
            return dtypes.Int32
        elif _type in ["int16", "smallint", "short"]:
            return dtypes.Int16
        elif _type in ["int8", "tinyint", "byte"]:
            return dtypes.Int8

        # Unsigned Integer types
        elif _type in ["uint128"]:
            return dtypes.UInt128
        elif _type in ["uint64"]:
            return dtypes.UInt64
        elif _type in ["uint32"]:
            return dtypes.UInt32
        elif _type in ["uint16"]:
            return dtypes.UInt16
        elif _type in ["uint8"]:
            return dtypes.UInt8

        # Floating-point types
        elif _type in ["float32", "float"]:
            return dtypes.Float32
        elif _type in ["float64", "double"]:
            return dtypes.Float64

        # String & Boolean
        elif _type in ["string", "str", "varchar", "text"]:
            return dtypes.String
        elif _type in ["boolean", "bool"]:
            return dtypes.Boolean

        # Special types
        elif _type in ["object", "obj", "binary", "varbinary"]:
            return dtypes.Object
        elif _type in ["categorical", "category"]:
            return dtypes.Categorical
        elif _type in ["enum"]:
            return dtypes.Enum

        # Date & Time types
        elif _type in ["datetime", "timestamp"]:
            return dtypes.Datetime
        elif _type in ["duration"]:
            return dtypes.Duration
        elif _type in ["date"]:
            return dtypes.Date

        # Complex types
        elif _type in ["field"]:
            return dtypes.Field
        elif _type in ["struct"]:
            return dtypes.Struct
        elif _type in ["list", "array"]:
            return dtypes.List if _type == "list" else dtypes.Array

        # Unknown
        elif _type in ["unknown"]:
            return dtypes.Unknown

        # Not Found
        else:
            raise ValueError(
                f"Data type '{self.type}' for '{self.name}' is not supported"
            )


class DataFrameSchema(BaseModel):
    columns: list[ColumnSchema]

    # Narwhals
    def to_narwhals(self):
        cols = {}
        for c in self.columns:
            cols[c.name] = c.nw_dtype
        return nw.Schema(cols)

    # Polars
    def to_polars(self):
        import polars as pl
        from narwhals._polars.utils import narwhals_to_native_dtype

        cols = {}
        for c in self.columns:
            cols[c.name] = narwhals_to_native_dtype(c.nw_dtype, nw.utils.Version.MAIN)
        return pl.Schema(cols)

    # Spark
    def to_spark(self):
        import pyspark.sql.types as T
        from narwhals._spark_like.utils import narwhals_to_native_dtype

        columns = []
        for c in self.columns:
            _type = narwhals_to_native_dtype(c.nw_dtype, nw.utils.Version.MAIN, T)
            columns += [T.StructField(c.name, _type, c.nullable)]

        return T.StructType(columns)

    # String
    def to_string(self, indent=None):
        return json.dumps({c.name: c.type for c in self.columns}, indent=indent)
