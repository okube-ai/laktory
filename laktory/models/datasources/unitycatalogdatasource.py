from narwhals import LazyFrame
from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.tabledatasource import TableDataSource

logger = get_logger(__name__)


class Connection(BaseModel):
    workspace_url: str
    bearer_token: str = "auto"
    require_https: bool = True


class UnityCatalogDataSource(TableDataSource):
    """
    Data source using a Unity Catalog data table.
    Generally used in the context of a data pipeline.

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

    catalog_name: str = None
    table_name: str = None
    schema_name: str = None
    # connection: Connection = None
    type: str = Field("UNITY_CATALOG", frozen=True)

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark=None) -> LazyFrame:
        if self.as_stream:
            logger.info(f"Reading {self._id} as stream")
            df = spark.readStream.table(self.full_name)
        else:
            logger.info(f"Reading {self._id} as static")
            df = spark.read.table(self.full_name)

        return df

    def _read_polars(self) -> LazyFrame:
        # TODO
        # https://docs.pola.rs/api/python/stable/reference/catalog/api/polars.Catalog.scan_table.html#polars.Catalog.scan_table
        raise NotImplementedError()
