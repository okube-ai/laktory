from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasources.tabledatasource import TableDataSource

logger = get_logger(__name__)


class HiveMetastoreDataSource(TableDataSource):
    """
    Data source using a Hive Metastore data table.

    Examples
    ---------
    ```python
    from laktory import models

    source = models.HiveMetastoreDataSource(
        schema_name="finance",
        table_name="brz_stock_prices",
    )
    # df = source.read()
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    # connection: Connection = None
    type: Literal["HIVE_METASTORE"] = Field(
        "HIVE_METASTORE", frozen=True, description="Source Type"
    )

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #
