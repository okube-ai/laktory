from __future__ import annotations

from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasinks.tabledatasink import TableDataSink

logger = get_logger(__name__)


class HiveMetastoreDataSink(TableDataSink):
    """
    Data sink writing to a Hive Metastore data table.

    Examples
    ---------
    ```python
    from laktory import models

    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])

    sink = models.HiveMetastoreDataSink(
        schema_name="my_schema",
        table_name="my_table",
    )
    sink.write(df)
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
