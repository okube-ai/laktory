from typing import Union

import narwhals as nw

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class DType(BaseModel):
    name: str  # TODO: Add Literal?
    inner: Union[str, "DType"] = None
    fields: dict[str, Union[str, "DType"]] = None

    @property
    def to_nw(self):
        dtypes = nw.dtypes
        _type = self.name.lower()

        # Array
        if _type in ["array"]:
            inner = self.inner
            if isinstance(inner, str):
                inner = DType(name=inner)
            return dtypes.List(inner=inner.to_nw)

        if _type in ["struct"]:
            fields = []
            for name, _dtype in self.fields.items():
                if isinstance(_dtype, str):
                    _dtype = DType(name=_dtype)
                fields += [nw.Field(name=name, dtype=_dtype.to_nw)]
            return dtypes.Struct(fields)

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

    @property
    def to_spark(self):
        import pyspark.sql.types as T
        from narwhals._spark_like.utils import narwhals_to_native_dtype

        return narwhals_to_native_dtype(self.to_nw, nw.utils.Version.MAIN, T)

    @property
    def to_polars(self):
        from narwhals._polars.utils import narwhals_to_native_dtype

        return narwhals_to_native_dtype(self.to_nw, nw.utils.Version.MAIN)
