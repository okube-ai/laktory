from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.spark import DataFrame

logger = get_logger(__name__)


class TableUnion(BaseModel):
    """
    Specifications of a tables union.

    Attributes
    ----------
    left:
        Left side of the join
    other:
        Right side of the join

    Examples
    --------
    ```py
    from laktory import models

    table = models.Table(
        name="slv_star_stock_prices",
        builder={
            "layer": "SILVER",
            "table_source": {
                "name": "slv_stock_prices_googl",
            },
            "unions": [
                {
                    "other": {
                        "name": "slv_stock_prices_aapl",
                    }
                },
                {
                    "other": {
                        "name": "slv_stock_prices_amzn",
                    }
                },
            ],
        },
    )
    ```

    References
    ----------

    * [pyspark union](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html)
    """

    left: TableDataSource = None
    other: TableDataSource

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
        logger.info(f"Executing {self.left.name} UNION {self.other.name}")

        left_df = self.left.read(spark)
        other_df = self.other.read(spark)

        logger.info(f"Left Schema:")
        left_df.printSchema()

        logger.info(f"Other Schema:")
        other_df.printSchema()

        df = left_df.union(other=other_df)

        return df
