from typing import Any
from typing import Literal
from typing import Union

import narwhals as nw
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)

NAMES = [
    "Array",
    "Boolean",
    "Categorical",
    "Date",
    "Datetime",
    "Decimal",
    "Duration",
    "Enum",
    "Float32",
    "Float64",
    "Int128",
    "Int16",
    "Int32",
    "Int64",
    "Int8",
    "List",
    "Object",
    "String",
    "Struct",
    "UInt128",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt8",
    "Unknown",
]

ALIASES = {
    "bigint": "Int64",
    "int": "Int32",
    "integer": "Int32",
    "smallint": "Int16",
    "short": "Int16",
    "tinyint": "Int8",
    "byte": "Int8",
    "float": "Float32",
    "double": "Float64",
    "str": "String",
    "varchar": "String",
    "text": "String",
    "bool": "Boolean",
    "obj": "Object",
    "binary": "Object",
    "varbinary": "Object",
    "category": "Categorical",
    "timestamp": "Datetime",
}


__all__ = ["DType", "DField"] + NAMES


class DField(BaseModel):
    """
    Data Field definition
    """

    name: str = Field(..., description="Field name")
    dtype: Union[str, "DType"] = Field(..., description="Field data type")

    @field_validator("dtype", mode="before")
    def update_dtype(cls, dtype: Any) -> Any:
        if isinstance(dtype, str):
            dtype = DType(name=dtype)
        return dtype


class DType(BaseModel):
    """
    Generic data type class.

    Examples
    --------
    ```
    from laktory import models

    # Int32
    dtype = models.DType(name="Int32")
    print(dtype)

    # List of string
    dtype = models.DType(name="list", inner="str")
    print(dtype)

    # Structure
    dtype = models.DType(
        name="struct", fields={"x": {"name": "list", "inner": "double"}, "y": dtype}
    )
    print(dtype)
    ```
    """

    name: str = Field(..., description="Data type name.")
    inner: Union[str, "DType"] = Field(
        None, description="Data type for sub-elements for `Array` or `List` types."
    )
    fields: list[DField] = Field(
        None, description="Definition of fields for `Struct` type."
    )
    shape: int | list[int] = Field(
        None, description="Definition of shape for `Array` type."
    )
    category: Literal["NUMERIC", "STRING", "STRUCT"] = Field(
        "NUMERIC", description="Data type category"
    )

    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, name: Any) -> Any:
        # From Names
        for _name in NAMES:
            if name.lower() == _name.lower():
                return _name

        # From Aliases
        _name = ALIASES.get(name.lower(), None)
        if _name:
            logger.warn(f"Data type '{name}' should be replaced with '{_name}'")
            return _name

        raise ValueError(f"'{name}' type is not supported.")

    @model_validator(mode="after")
    def complex_types(self) -> Any:
        if self.name in ["Array", "List"] and self.inner is None:
            raise ValueError(f"'{self.name}' type requires `inner` value.")
        if self.name in ["Array"] and self.shape is None:
            raise ValueError(f"'{self.name}' type requires `shape` value.")
        if self.name in ["Struct"] and self.fields is None:
            raise ValueError(f"'{self.name}' type requires `fields` value.")
        return self

    @field_validator("inner", mode="before")
    def update_inner(cls, v: Any) -> Any:
        if isinstance(v, str):
            return DType(name=v)
        if isinstance(v, DType) and type(v) is not DType:
            """Required to prevent serialization issue with pydantic"""
            dump = v.model_dump(exclude_unset=True)  # Required to avoid None in inner
            return DType(**dump)

        return v

    # @field_validator("fields", mode="before")
    # def update_fields(cls, fields: Any) -> Any:
    #     if fields is None:
    #         return
    #
    #     for k, v in fields.items():
    #         if isinstance(v, str):
    #             fields[k] = DType(name=v)
    #
    #         elif isinstance(v, DType) and type(v) is not DType:
    #             """Required to prevent serialization issue with pydantic"""
    #             dump = v.model_dump(exclude_unset=True)
    #             fields[k] = DType(**dump)
    #
    #     return fields

    def to_generic(self):
        return DType(**self.model_dump(exclude_unset=True))

    def to_narwhals(self):
        """Get equivalent Narwhals data type"""
        nw_dtypes = nw.dtypes
        _type = self.name

        # Complex types
        if _type in "Array":
            return nw_dtypes.Array(inner=self.inner.to_narwhals(), shape=self.shape)

        if _type == "List":
            return nw_dtypes.List(inner=self.inner.to_narwhals())

        if _type == "Struct":
            fields = []
            for field in self.fields:
                fields += [nw.Field(name=field.name, dtype=field.dtype.to_narwhals())]
            return nw_dtypes.Struct(fields)

        if hasattr(nw_dtypes, _type):
            return getattr(nw_dtypes, _type)

        # Not Found
        raise ValueError(f"Data type with name '{self.name}' is not supported")

    def to_spark(self):
        """Get equivalent Spark data type"""
        import pyspark.sql.types as T
        from narwhals._spark_like.utils import narwhals_to_native_dtype

        from laktory import get_spark_session

        spark = get_spark_session()
        return narwhals_to_native_dtype(
            dtype=self.to_narwhals(),
            version=nw._utils.Version.MAIN,
            spark_types=T,
            session=spark,
        )

    def to_polars(self):
        """Get equivalent Polars data type"""
        from narwhals._polars.utils import narwhals_to_native_dtype

        return narwhals_to_native_dtype(
            dtype=self.to_narwhals(),
            version=nw._utils.Version.MAIN,
        )

    def to_string(self):
        return str(self.to_polars())


class SpecificDType(DType):
    @model_validator(mode="after")
    def set_name(self) -> Any:
        """Force dump of `name` property to allow de-serialization from DType model"""
        self.model_fields_set.add("name")
        return self


# Complex types
class Array(SpecificDType):
    name: str = Field("Array", frozen=True)
    inner: str | DType
    shape: int | list[int]


class List(SpecificDType):
    name: str = Field("List", frozen=True)
    inner: str | DType


class Struct(SpecificDType):
    name: str = Field("Struct", frozen=True)
    fields: list[DField]


# Integer types
class Decimal(SpecificDType):
    name: str = Field("Decimal", frozen=True)


class Int128(SpecificDType):
    name: str = Field("Int128", frozen=True)


class Int64(SpecificDType):
    name: str = Field("Int64", frozen=True)


class Int32(SpecificDType):
    name: str = Field("Int32", frozen=True)


class Int16(SpecificDType):
    name: str = Field("Int16", frozen=True)


class Int8(SpecificDType):
    name: str = Field("Int8", frozen=True)


# Unsigned Integer types
class UInt128(SpecificDType):
    name: str = Field("UInt128", frozen=True)


class UInt64(SpecificDType):
    name: str = Field("UInt64", frozen=True)


class UInt32(SpecificDType):
    name: str = Field("UInt32", frozen=True)


class UInt16(SpecificDType):
    name: str = Field("UInt16", frozen=True)


class UInt8(SpecificDType):
    name: str = Field("UInt8", frozen=True)


# Floating-point types
class Float32(SpecificDType):
    name: str = Field("Float32", frozen=True)


class Float64(SpecificDType):
    name: str = Field("Float64", frozen=True)


# String & Boolean
class String(SpecificDType):
    name: str = Field("String", frozen=True)


class Boolean(SpecificDType):
    name: str = Field("Boolean", frozen=True)


# Special types
class Object(SpecificDType):
    name: str = Field("Object", frozen=True)


class Categorical(SpecificDType):
    name: str = Field("Categorical", frozen=True)


class Enum(SpecificDType):
    name: str = Field("Enum", frozen=True)


# Date & Time types
class Datetime(SpecificDType):
    name: str = Field("Datetime", frozen=True)


class Duration(SpecificDType):
    name: str = Field("Duration", frozen=True)


class Date(SpecificDType):
    name: str = Field("Date", frozen=True)


# Unknown
class Unknown(SpecificDType):
    name: str = Field("Unknown", frozen=True)


DType.model_rebuild()
DField.model_rebuild()
