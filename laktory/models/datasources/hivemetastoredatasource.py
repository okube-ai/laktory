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
        Name (or full name) of the source table. If full name ({catalog}.{schema}.{table})
        is provided, `schema_name` and `catalog_name` will be overwritten.

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

    # connection: Connection = None
    type: str = Field("HIVE_METASTORE", frozen=True)

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #
