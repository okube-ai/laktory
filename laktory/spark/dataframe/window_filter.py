from pydantic import BaseModel
from typing import Union
from pyspark.sql.dataframe import DataFrame

from laktory._logger import get_logger
from laktory.models.spark.sparkcolumnnode import SparkColumnNode


logger = get_logger(__name__)


class OrderBy(BaseModel):
    sql_expression: str
    desc: bool = False


def window_filter(
    df,
    partition_by: Union[list[str], None],
    order_by: Union[list[OrderBy], None] = None,
    drop_row_index: bool = True,
    row_index_name: str = "_row_index",
    rows_to_keep: int = 1,
) -> DataFrame:
    """
    Window-based filtering

    Parameters
    ----------
    df:
        DataFrame
    partition_by
        Defines the columns used for grouping into Windows
    order_by:
        Defines the column used for sorting before dropping rows
    drop_row_index:
        If `True`, the row index column is dropped
    row_index_name:
        Group-specific and sorted rows index name
    rows_to_keep:
        How many rows to keep per window


    Examples
    --------
    ```py
    from datetime import datetime

    df0 = spark.createDataFrame(
        [
            [datetime(2023, 1, 1), "APPL", 200.0],
            [datetime(2023, 1, 2), "APPL", 202.0],
            [datetime(2023, 1, 3), "APPL", 201.5],
            [datetime(2023, 1, 1), "GOOL", 200.0],
            [datetime(2023, 1, 2), "GOOL", 202.0],
            [datetime(2023, 1, 3), "GOOL", 201.5],
        ],
        ["created_at", "symbol", "price"],
    )

    df = df0.window_filter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=1,
    )

    print(df.toPandas().to_string())
    '''
               created_at symbol  price  _row_index
    0 2023-01-03 05:00:00   APPL  201.5           1
    1 2023-01-03 05:00:00   GOOL  201.5           1
    '''
    ```

    References
    ----------

    * [pyspark window](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.window.html)
    """
    from pyspark.sql import Window
    import pyspark.sql.functions as F

    # Partition by
    w = Window.partitionBy(*partition_by)

    # Order by
    if order_by:
        order_bys = []
        for o in order_by:
            if not isinstance(o, OrderBy):
                o = OrderBy(**o)
            order_bys += [F.expr(o.sql_expression)]
            if o.desc:
                order_bys[-1] = order_bys[-1].desc()
        w = w.orderBy(*order_bys)

    # Set rows index
    df = df.withColumn(row_index_name, F.row_number().over(w))

    # Filter
    df = df.filter(F.col(row_index_name) <= rows_to_keep)
    if drop_row_index:
        df = df.drop(row_index_name)

    return df
