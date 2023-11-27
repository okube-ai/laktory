from typing import Union

from laktory._logger import get_logger
from laktory.models.base import BaseModel

logger = get_logger(__name__)


class OrderBy(BaseModel):
    sql_expression: str
    desc: bool = False


class TableWindowFilter(BaseModel):
    partition_by: Union[list[str], None]
    order_by: Union[list[OrderBy], None] = None
    descending: bool = True
    rows_to_keep: int = 1
    drop_row_index: bool = True
    row_index_name: str = "_row_index"

    def run(self, df):
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
