from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals as nw
from pydantic_core import CoreSchema
from pydantic_core import core_schema

# from laktory.polars import PolarsExpr
# from laktory.polars import PolarsLazyFrame
# from laktory.spark import SparkColumn
# from laktory.spark import SparkDataFrame

if TYPE_CHECKING:
    pass

AnyFrame = nw.LazyFrame | nw.DataFrame

# AnyExpression = nw.Expr | "pl.Expr" | "F.Column"

# # TODO: Delete
# AnyDataFrame = SparkDataFrame | PolarsLazyFrame
# """DataFrame type from any of the supported backend"""
#
# # TODO: Delete
# AnyDataFrameColumn = SparkColumn | PolarsExpr
# """DataFrame column from any of the supported backend"""


class VariableType(str):
    """Laktory variable or expression (string)"""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type, handler: callable
    ) -> CoreSchema:
        return core_schema.str_schema()


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
