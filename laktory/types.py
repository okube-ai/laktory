from typing import Union
from laktory.spark import SparkDataFrame
from laktory.spark import SparkColumn
from laktory.polars import PolarsDataFrame
from laktory.polars import PolarsExpr

AnyDataFrame = Union[SparkDataFrame, PolarsDataFrame]
AnyDataFrameColumn = Union[SparkColumn, PolarsExpr]
