from typing import Any
from typing import Union

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)

ALL_NAMES = [
    # Complex
    "array",
    "list",
    "struct",
    # Integer
    "decimal",
    "int128",
    "int64",
    "bigint",
    "int32",
    "int",
    "integer",
    "int16",
    "smallint",
    "short" "int8",
    "tinyint",
    "byte",
    # Unsigned Integer
    "uint128",
    "uint64",
    "uint32",
    "uint16",
    "uint8",
    # Floating-point
    "float32",
    "float",
    "float64",
    "double",
    # String & Boolean
    "string",
    "str",
    "varchar",
    "text",
    "boolean",
    "bool",
    # Special types
    "object",
    "obj",
    "binary",
    "varbinary",
    "categorical",
    "category",
    "enum",
    # Date & Time
    "datetime",
    "timestamp",
    "duration",
    "date",
    # Unknown
    "unknown",
]


class DType(BaseModel):
    name: str
    inner: Union[str, "DType"] = None
    fields: dict[str, Union[str, "DType"]] = None
    shape: Union[int, list[int]] = None

    @model_validator(mode="after")
    def validate_names(self):
        if self.name.lower() not in ALL_NAMES:
            raise ValueError(f"'{self.name}' type is not supported.")
        return self

    @model_validator(mode="after")
    def complex_types(self) -> Any:
        if self.name.lower() in ["array", "list"] and self.inner is None:
            raise ValueError(f"'{self.name}' type requires `inner` value.")
        if self.name.lower() in ["array"] and self.shape is None:
            raise ValueError(f"'{self.name}' type requires `shape` value.")
        if self.name.lower() in ["struct"] and self.fields is None:
            raise ValueError(f"'{self.name}' type requires `fields` value.")
        return self

    @property
    def to_nw(self):
        dtypes = nw.dtypes
        _type = self.name.lower()

        # Complex types
        if _type in ["array"]:
            inner = self.inner
            if isinstance(inner, str):
                inner = DType(name=inner)
            return dtypes.Array(inner=inner.to_nw, shape=self.shape)

        if _type in ["list"]:
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
        import polars as pl
        from narwhals._polars.utils import narwhals_to_native_dtype
        from narwhals.utils import parse_version

        pl_version = parse_version(pl)

        return narwhals_to_native_dtype(
            dtype=self.to_nw, version=nw.utils.Version.MAIN, backend_version=pl_version
        )

    @property
    def to_string(self):
        return str(self.to_polars)


# Complex types
class Array(DType):
    name: str = Field("array", frozen=True)
    inner: Union[str, DType]
    shape: Union[int, list[int]] = None


class List(DType):
    name: str = Field("list", frozen=True)
    inner: Union[str, DType]


class Struct(DType):
    name: str = Field("struct", frozen=True)
    fields: dict[str, Union[str, DType]]


# Integer types
class Decimal(DType):
    name: str = Field("decimal", frozen=True)


class Int128(DType):
    name: str = Field("int128", frozen=True)


class Int64(DType):
    name: str = Field("int64", frozen=True)


class Int32(DType):
    name: str = Field("int32", frozen=True)


class Int16(DType):
    name: str = Field("int16", frozen=True)


class Int8(DType):
    name: str = Field("int8", frozen=True)


# Unsigned Integer types
class UInt128(DType):
    name: str = Field("uint128", frozen=True)


class UInt64(DType):
    name: str = Field("uint64", frozen=True)


class UInt32(DType):
    name: str = Field("uint32", frozen=True)


class UInt16(DType):
    name: str = Field("uint16", frozen=True)


class UInt8(DType):
    name: str = Field("uint8", frozen=True)


# Floating-point types
class Float32(DType):
    name: str = Field("float32", frozen=True)


class Float64(DType):
    name: str = Field("float64", frozen=True)


# String & Boolean
class String(DType):
    name: str = Field("string", frozen=True)


class Boolean(DType):
    name: str = Field("boolean", frozen=True)


# Special types
class Object(DType):
    name: str = Field("object", frozen=True)


class Categorical(DType):
    name: str = Field("categorical", frozen=True)


class Enum(DType):
    name: str = Field("enum", frozen=True)


# Date & Time types
class Datetime(DType):
    name: str = Field("datetime", frozen=True)


class Duration(DType):
    name: str = Field("duration", frozen=True)


class Date(DType):
    name: str = Field("date", frozen=True)


# Unknown
class Unknown(DType):
    name: str = Field("unknown", frozen=True)
