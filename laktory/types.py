from typing import Union
from laktory.spark import SparkDataFrame
from laktory.spark import SparkColumn
from laktory.polars import PolarsLazyFrame
from laktory.polars import PolarsExpr

AnyDataFrame = Union[SparkDataFrame, PolarsLazyFrame]
AnyDataFrameColumn = Union[SparkColumn, PolarsExpr]
