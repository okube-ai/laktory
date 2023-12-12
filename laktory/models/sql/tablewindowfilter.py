from typing import Union

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class OrderBy(BaseModel):
    sql_expression: str
    desc: bool = False


class TableWindowFilter(BaseModel):
    """
    Specifications of a window-based filtering.

    Attributes
    ----------
    descending:
        If `True` rows are sorted in descending order before dropping rows
        from each window.
    drop_row_index:
        If `True`, the row index column is dropped
    order_by:
        Defines the column used for sorting before dropping rows
    partition_by
        Defines the columns used for grouping into Windows
    row_index_name:
        Group-specific and sorted rows index name
    rows_to_keep:
        How many rows to keep per window

    Examples
    --------
    ```py
    from datetime import datetime
    from laktory import models

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

    f = models.TableWindowFilter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=1,
    )
    df1 = f.execute(df0)
    ```

    References
    ----------

    * [pyspark Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)
    """

    descending: bool = True
    drop_row_index: bool = True
    order_by: Union[list[OrderBy], None] = None
    partition_by: Union[list[str], None]
    row_index_name: str = "_row_index"
    rows_to_keep: int = 1

    def execute(self, df):
        """
        Execute window filtering

        Parameters
        ----------
        df: DataFrame
            Spark DataFrame

        Returns
        -------
        : DataFrame
            Output DataFrame
        """
        from pyspark.sql import Window
        import pyspark.sql.functions as F

        # Partition by
        w = Window.partitionBy(*self.partition_by)

        # Order by
        if self.order_by:
            order_bys = []
            for o in self.order_by:
                order_bys += [F.expr(o.sql_expression)]
                if o.desc:
                    order_bys[-1] = order_bys[-1].desc()
            w = w.orderBy(*order_bys)

        # Set rows index
        df = df.withColumn(self.row_index_name, F.row_number().over(w))

        # Filter
        df = df.filter(F.col(self.row_index_name) <= self.rows_to_keep)
        if self.drop_row_index:
            df = df.drop(self.row_index_name)

        return df


if __name__ == "__main__":
    from laktory import models

    f = models.TableWindowFilter(
        partition_by=["symbol"],
        order_by=[
            {"sql_expression": "created_at", "desc": True},
        ],
        drop_row_index=False,
        rows_to_keep=1,
    )
    df1 = f.execute(df0)
