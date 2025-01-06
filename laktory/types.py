from typing import Union

from laktory.polars import PolarsExpr
from laktory.polars import PolarsLazyFrame
from laktory.spark import SparkColumn
from laktory.spark import SparkDataFrame

AnyDataFrame = Union[SparkDataFrame, PolarsLazyFrame]
AnyDataFrameColumn = Union[SparkColumn, PolarsExpr]
