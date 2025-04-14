from typing import Literal

from narwhals import LazyFrame
from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasinks.tabledatasink import TableDataSink

logger = get_logger(__name__)


class UnityCatalogDataSink(TableDataSink):
    """
    Data sink using a Unity Catalog data table.

    Examples
    ---------
    ```python
    from laktory import models

    source = models.UnityCatalogDataSource(
        catalog_name="dev",
        schema_name="finance",
        table_name="brz_stock_prices",
        selects=["symbol", "open", "close"],
        filter="symbol='AAPL'",
        as_stream=True,
    )
    # df = source.read()
    ```
    """

    type: Literal["UNITY_CATALOG"] = Field(
        "UNITY_CATALOG", frozen=True, description="Sink Type"
    )

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_polars(self, df) -> LazyFrame:
        # TODO
        # https://docs.pola.rs/api/python/stable/reference/catalog/api/polars.Catalog.scan_table.html#polars.Catalog.scan_table
        raise NotImplementedError(
            "Unity Catalog data sink is not yet implemented for Polars"
        )
