from pydantic import field_validator
from pydantic import model_validator
from typing import Union
from typing import Any
from typing import Callable

from laktory._logger import get_logger
from laktory.models.base import BaseModel
from laktory.spark import DataFrame
from laktory.spark import Column as SparkColumn
from laktory.models.sql.column import Column

logger = get_logger(__name__)


class Window(BaseModel):
    time_column: str
    window_duration: str
    slide_duration: Union[str, None] = None
    start_time: Union[str, None] = None


# TODO: Add support for partition by and drop
"""
w = Window.partitionBy("account_id").orderBy(F.col("created_at").desc())
df = df.withColumn("_row", F.row_number().over(w)).filter(F.col("_row") == 1)
df = df.drop("_row")
"""


class TableAggregation(BaseModel):
    groupby_window: Union[Window, None] = None
    groupby_columns: Union[list[str], None] = []
    agg_expressions: list[Column] = []

    @model_validator(mode="after")
    def groupby(self) -> Any:
        if self.groupby_window is None and len(self.groupby_columns) == 0:
            raise ValueError(
                "Either `groupby_window` or `groupby_columns` must be specified."
            )

        return self

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def run(
        self,
        df,
        udfs: list[Callable[[...], SparkColumn]] = None,
    ) -> DataFrame:
        import pyspark.sql.functions as F

        # Groupby arguments
        groupby = []
        if self.groupby_window:
            groupby += [
                F.window(
                    timeColumn=self.groupby_window.time_column,
                    windowDuration=self.groupby_window.window_duration,
                    slideDuration=self.groupby_window.slide_duration,
                    startTime=self.groupby_window.start_time,
                )
            ]

        for c in self.groupby_columns:
            groupby += [c]

        # Agg arguments
        aggs = []
        for expr in self.agg_expressions:
            expr.type = "_any"
            aggs += [
                expr.to_spark(
                    df=df,
                    udfs=udfs,
                    raise_exception=True,
                ).alias(expr.name)
            ]

        return df.groupby(groupby).agg(*aggs)
