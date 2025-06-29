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
    import laktory as lk

    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])

    sink = lk.models.HiveMetastoreDataSink(
        schema_name="default",
        table_name="my_table",
        mode="APPEND",
    )
    # sink.write(df)
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
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
