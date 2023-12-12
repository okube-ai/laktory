from typing import Union

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.spark import DataFrame

logger = get_logger(__name__)


class TableJoin(BaseModel):
    """
    Specifications of a tables join.

    Attributes
    ----------
    how:
        Type of join (left, outer, full, etc.)
    left:
        Left side of the join
    on:
        A list of strings for the columns to join on. The columns must exist
        on both sides.
    other:
        Right side of the join
    time_constraint_interval_lower:
        Lower bound for a spark streaming event-time constraint
    time_constraint_interval_upper:
        Upper bound for a spark streaming event-time constraint

    Examples
    --------
    ```py
    from laktory import models

    table = models.Table(
        name="slv_star_stock_prices",
        builder={
            "layer": "SILVER",
            "table_source": {
                "name": "slv_stock_prices",
            },
            "joins": [
                {
                    "other": {
                        "name": "slv_stock_metadata",
                        "read_as_stream": False,
                        "selects": ["symbol", "currency", "first_trader"],
                    },
                    "on": ["symbol"],
                }
            ],
        },
    )
    ```

    References
    ----------

    * [pyspark join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)
    * [spark streaming join](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#inner-joins-with-optional-watermarking)
    """

    how: str = "left"
    left: TableDataSource = None
    on: list[str]
    other: TableDataSource
    time_constraint_interval_lower: str = "60 seconds"
    time_constraint_interval_upper: str = None

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def execute(self, spark) -> DataFrame:
        """
        Execute join

        Parameters
        ----------
        spark: SparkSession
            Spark session

        Returns
        -------
        : DataFrame
            Output DataFrame
        """
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

        # Clean join columns
        for c in self.on:
            df = df.withColumn("__tmp", F.coalesce(f"left.{c}", f"other.{c}"))
            df = df.drop(c)
            df = df.withColumn(c, F.col("__tmp"))
            df = df.drop("__tmp")

        # Drop watermark column
        if self.other.watermark is not None:
            df = df.drop(F.col(f"other._other_wc"))
        logger.info(f"Joined Schema:")
        df.printSchema()

        return df


if __name__ == "__main__":
    from laktory import models

    table = models.Table(
        name="slv_star_stock_prices",
        builder={
            "layer": "SILVER",
            "table_source": {
                "name": "slv_stock_prices",
            },
            "joins": [
                {
                    "other": {
                        "name": "slv_stock_metadata",
                        "read_as_stream": False,
                        "selects": ["symbol", "currency", "first_trader"],
                    },
                    "on": ["symbol"],
                }
            ],
        },
    )

    # Read
    df = table.builder.read_source(spark)

    # Process
    df = table.builder.process(df, spark)
