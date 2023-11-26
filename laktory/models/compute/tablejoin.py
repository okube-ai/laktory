from typing import Union

from laktory._logger import get_logger
from laktory.models.base import BaseModel
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.spark import DataFrame

logger = get_logger(__name__)


class TableJoin(BaseModel):
    left: TableDataSource = None
    other: TableDataSource
    on: list[str]
    how: str = "left"
    time_constraint_interval_lower: str = "60 seconds"
    time_constraint_interval_upper: str = None

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def run(self, spark) -> DataFrame:
        import pyspark.sql.functions as F

        logger.info(f"Executing {self.left.name} {self.how} JOIN {self.other.name}")

        left_df = self.left.read(spark)
        other_df = self.other.read(spark)

        # Add watermark
        other_cols = []
        if self.other.watermark is not None:
            other_cols += [F.col(self.other.watermark.column).alias("_other_wc")]
        other_cols += [F.col(c) for c in other_df.columns]
        other_df = other_df.select(other_cols)

        # Drop duplicates to prevent adding rows to left
        other_df = other_df.dropDuplicates(self.on)

        _join = []
        for c in self.on:
            _join += [f"left.{c} == other.{c}"]
        if self.other.watermark is not None:
            if self.time_constraint_interval_lower:
                _join += [
                    f"left.{self.left.watermark.column} >= other._other_wc - interval {self.time_constraint_interval_lower}"
                ]
            if self.time_constraint_interval_upper:
                _join += [
                    f"left.{self.left.watermark.column} <= other._other_wc + interval {self.time_constraint_interval_upper}"
                ]
        _join = " AND ".join(_join)

        logger.info(f"   ON {_join}")

        logger.info(f"Left Schema:")
        left_df.printSchema()

        logger.info(f"Other Schema:")
        other_df.printSchema()

        df = (
            left_df.alias("left")
            .join(
                other=other_df.alias("other"),
                on=F.expr(_join),
                how=self.how,
            )
            .drop()
        )

        # Drop join columns
        for c in self.on:
            df = df.drop(F.col(f"other.{c}"))
        if self.other.watermark is not None:
            df = df.drop(F.col(f"other._other_wc"))
        logger.info(f"Joined Schema:")
        df.printSchema()

        return df
