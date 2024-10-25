from pydantic import BaseModel
from typing import Union
from typing import TYPE_CHECKING
from pyspark.sql.dataframe import DataFrame

from laktory._logger import get_logger

if TYPE_CHECKING:
    from laktory.models.transformers.basechainnode import ChainNodeColumn


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


def groupby_and_agg(
    df,
    groupby_window: TimeWindow = None,
    groupby_columns: list[str] = None,
    agg_expressions: list["ChainNodeColumn"] = None,
) -> DataFrame:
    """
    Apply a groupby and create aggregation columns.

    Parameters
    ----------
    df:
        DataFrame
    groupby_window:
        Aggregation window definition
    groupby_columns:
        List of column names to group by
    agg_expressions:
        List of columns defining the aggregations

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import pandas as pd

    df0 = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "price": [200.0, 205.0],
                "tstamp": ["2023-09-01", "2023-09-01"],
            }
        )
    )

    df = df0.laktory.groupby_and_agg(
        groupby_window={
            "time_column": "tstamp",
            "window_duration": "1 day",
        },
        agg_expressions=[
            {
                "name": "mean_price",
                "expr": "F.mean('price')",
            },
        ],
    )

    print(df.toPandas().to_string())
    '''
                                           window  mean_price
    0  (2023-09-01 00:00:00, 2023-09-02 00:00:00)       202.5
    '''
    ```

    References
    ----------

    * [pyspark window](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.window.html)
    """
    import pyspark.sql.functions as F
    from laktory.models.transformers.basechainnode import ChainNodeColumn

    # Parse inputs
    if groupby_window and not isinstance(groupby_window, TimeWindow):
        groupby_window = TimeWindow(**groupby_window)
    if agg_expressions is None:
        raise ValueError("`agg_expressions` must be specified")
    if groupby_columns is None:
        groupby_columns = []

    logger.info(
        f"Executing groupby ({groupby_window} & {groupby_columns}) with {agg_expressions}"
    )

    # Groupby arguments
    groupby = []
    if groupby_window:
        groupby += [
            F.window(
                timeColumn=groupby_window.time_column,
                windowDuration=groupby_window.window_duration,
                slideDuration=groupby_window.slide_duration,
                startTime=groupby_window.start_time,
            )
        ]

    for c in groupby_columns:
        groupby += [c]

    # Agg arguments
    aggs = []
    for expr in agg_expressions:
        if not isinstance(expr, ChainNodeColumn):
            expr = ChainNodeColumn(**expr)

        expr.type = None
        aggs += [expr.eval(dataframe_type="SPARK").alias(expr.name)]

    return df.groupby(groupby).agg(*aggs)
