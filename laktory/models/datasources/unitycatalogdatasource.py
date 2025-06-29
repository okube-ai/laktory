from typing import Literal

from narwhals import LazyFrame
from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasources.tabledatasource import TableDataSource

logger = get_logger(__name__)


# class Connection(BaseModel):
#     workspace_url: str
#     bearer_token: str = "auto"
#     require_https: bool = True


class UnityCatalogDataSource(TableDataSource):
    """
    Data source using a Unity Catalog data table.

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
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    # connection: Connection = None
    type: Literal["UNITY_CATALOG"] = Field(
        "UNITY_CATALOG", frozen=True, description="Source Type"
    )

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_polars(self) -> LazyFrame:
        # TODO
        # https://docs.pola.rs/api/python/stable/reference/catalog/api/polars.Catalog.scan_table.html#polars.Catalog.scan_table
        raise NotImplementedError(
            "Unity Catalog data source is not yet implemented for Polars"
        )
