from typing import Union

import narwhals as nw
from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasources.tabledatasource import TableDataSource

logger = get_logger(__name__)


class HiveMetastoreDataSource(TableDataSource):
    """
    Data source using a Hive Metastore data table, generally used in the
    context of a data pipeline.

    Attributes
    ----------
    catalog_name:
        Name of the catalog of the source table
    schema_name:
        Name of the schema of the source table
    table_name:
        Name of the source table

    Examples
    ---------
    ```python
    from laktory import models

    source = models.TableDataSource(
        catalog_name="dev",
        schema_name="finance",
        table_name="brz_stock_prices",
        selects=["symbol", "open", "close"],
        filter="symbol='AAPL'",
        as_stream=True,
    )
    # df = source.read(spark)
    ```
    """

    table_name: Union[str, None] = None
    schema_name: Union[str, None] = None
    # connection: Connection = None
    type: str = Field("HIVE_METASTORE", frozen=True)

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark=None) -> nw.LazyFrame:
        if self.as_stream:
            logger.info(f"Reading {self._id} as stream")
            df = spark.readStream.table(self.full_name)
        else:
            logger.info(f"Reading {self._id} as static")
            df = spark.read.table(self.full_name)

        return nw.from_native(df)
