from typing import Union
from laktory.spark import SparkDataFrame
from laktory.polars import PolarsDataFrame

AnyDataFrame = Union[SparkDataFrame, PolarsDataFrame]
