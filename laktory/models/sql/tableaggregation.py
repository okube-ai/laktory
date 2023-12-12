from pydantic import field_validator
from pydantic import model_validator
from typing import Union
from typing import Any
from typing import Callable

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.spark import DataFrame
from laktory.spark import Column as SparkColumn
from laktory.models.sql.column import Column

logger = get_logger(__name__)


class TimeWindow(BaseModel):
    """
    Specifications for Time Window Aggregation

    Attributes
    ----------
    time_column:
        Timestamp column used for grouping rows
    window_duration:
        Duration of the window e.g. ‘1 second’, ‘1 day 12 hours’, ‘2 minutes’
    slide_duration
        Duration of the slide. If a slide is smaller than the window, windows
        are overlapping
    start_time
        Offset with respect to 1970-01-01 00:00:00 UTC with which to start
        window intervals.

    References
    ----------

    * [spark structured streaming windows](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#types-of-time-windows)
    * [pyspark window](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.window.html)

    """

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
    """
    Specifications for aggregation by group or time window.

    Attributes
    ----------
    groupby_window:
        Aggregation window definition
    groupby_columns:
        List of column names to group by
    agg_expressions:
        List of columns defining the aggregations

    Examples
    --------
    ```py
    from laktory import models

    table = models.Table(
        name="gld_stock_prices_by_1d",
        builder={
            "layer": "GOLD",
            "table_source": {
                "name": "slv_star_stock_prices",
            },
            "aggregation": {
                "groupby_columns": ["symbol"],
                "groupby_window": {
                    "time_column": "_tstamp",
                    "window_duration": "1 day",
                },
                "agg_expressions": [
                    {"name": "low", "spark_func_name": "min", "spark_func_args": ["low"]},
                    {"name": "high", "spark_func_name": "max", "spark_func_args": ["high"]},
                ],
            },
        },
    )
    ```


    References
    ----------

    * [pyspark window](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.window.html)
    """

    groupby_window: Union[TimeWindow, None] = None
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

    def execute(
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


if __name__ == "__main__":
    from laktory import models

    table = models.Table(
        name="gld_stock_prices_by_1d",
        builder={
            "layer": "GOLD",
            "table_source": {
                "name": "slv_star_stock_prices",
            },
            "aggregation": {
                "groupby_columns": ["symbol"],
                "groupby_window": {
                    "time_column": "_tstamp",
                    "window_duration": "1 day",
                },
                "agg_expressions": [
                    {
                        "name": "low",
                        "spark_func_name": "min",
                        "spark_func_args": ["low"],
                    },
                    {
                        "name": "high",
                        "spark_func_name": "max",
                        "spark_func_args": ["high"],
                    },
                ],
            },
        },
    )
