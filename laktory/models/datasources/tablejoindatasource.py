from typing import Union

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.spark import DataFrame

logger = get_logger(__name__)


class TableJoinDataSource(BaseDataSource):
    left: TableDataSource = None
    other: TableDataSource
    on: list[str]
    columns: Union[dict, list]
    how: str = "left"
    time_constraint_interval_lower: str = "60 seconds"
    time_constraint_interval_upper: str = None

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def read(self, spark) -> DataFrame:

        import pyspark.sql.functions as F

        left = self.left.read(spark)
        other = self.other.read(spark)

        # Filter and columns selection
        cols = self.columns

        other_cols = []
        if self.other.watermark is not None:
            other_cols += [F.col(self.other.watermark.column).alias("_other_wc")]
        if isinstance(cols, list):
            other_cols += [F.col(c) for c in cols]
        elif isinstance(cols, dict):
            other_cols += [F.col(k).alias(v) for k, v in cols.items()]
        other = other.select(other_cols)

        # Drop duplicates to prevent adding rows to left
        other = other.dropDuplicates(self.on)

        _join = []
        for c in self.on:
            _join += [f"left.{c} == other.{c}"]
        if self.other.watermark is not None:
            if self.time_constraint_interval_lower:
                _join += [f"left.{self.left.watermark.column} >= other._other_wc - interval {self.time_constraint_interval_lower}"]
            if self.time_constraint_interval_upper:
                _join += [f"left.{self.left.watermark.column} <= other._other_wc + interval {self.time_constraint_interval_upper}"]
        _join = " AND ".join(_join)

        logger.info(f"on statement: {_join}")

        df = left.alias("left").join(
            other=other.alias("other"),
            on=F.expr(_join),
            how=self.how,
        )

        # Drop join columns
        for c in self.on:
            df = df.drop(getattr(other, c))
        if self.other.watermark is not None:
            df = df.drop(getattr(other, "_other_wc"))

        return df
