from typing import Union

from pydantic_core import CoreSchema
from pydantic_core import core_schema

from laktory.polars import PolarsExpr
from laktory.polars import PolarsLazyFrame
from laktory.spark import SparkColumn
from laktory.spark import SparkDataFrame

AnyDataFrame = Union[SparkDataFrame, PolarsLazyFrame]
"""DataFrame type from any of the supported backend"""

AnyDataFrameColumn = Union[SparkColumn, PolarsExpr]
"""DataFrame column from any of the supported backend"""


class VariableType(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type, handler: callable
    ) -> CoreSchema:
        return core_schema.str_schema()


var = VariableType
"""Laktory variable or expression (string)"""

# ResolvableBool: TypeAlias = Union[bool, var]
# """Boolean or laktory variable that can be resolved as a boolean"""
#
# ResolvableFloat: TypeAlias = Union[float, var]
# """Float or laktory variable that can be resolved as a float"""
#
# ResolvableInt: TypeAlias = Union[int, var]
# """Int or laktory variable that can be resolved as an int"""
#
# ResolvableString: TypeAlias = Union[str, var]
# """String or laktory variable that can be resolved as a string"""
