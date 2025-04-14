from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasinks.tabledatasink import TableDataSink

logger = get_logger(__name__)


class HiveMetastoreDataSink(TableDataSink):
    """
    Data source using a Hive Metastore data table.

    Examples
    ---------
    ```python
    from laktory import models

    sink = models.HiveMetastoreDataSink(
        schema_name="finance",
        table_name="brz_stock_prices",
    )
    # sink.write(df)
    ```
    """

    # connection: Connection = None
    type: Literal["HIVE_METASTORE"] = Field(
        "HIVE_METASTORE", frozen=True, description="Sink Type"
    )

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #
