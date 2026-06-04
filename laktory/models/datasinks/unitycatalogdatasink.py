from typing import TYPE_CHECKING
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.resources.databricks.dataqualitymonitor_base import (
    DataQualityMonitorDataProfilingConfig,
)

if TYPE_CHECKING:
    from laktory.models.resources.databricks.dataqualitymonitor import (
        DataQualityMonitor,
    )

logger = get_logger(__name__)


class UnityCatalogDataSink(TableDataSink):
    """
    Data sink writing to a Unity Catalog data table

    Examples
    ---------
    ```python tag:skip-run
    from laktory import models

    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])

    sink = models.UnityCatalogDataSink(
        catalog_name="dev",
        schema_name="my_schema",
        table_name="my_stable",
        mode="OVERWRITE",
    )
    sink.write(df)
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    type: Literal["UNITY_CATALOG"] = Field(
        "UNITY_CATALOG", frozen=True, description="Sink Type"
    )
    databricks_data_profiling_config: DataQualityMonitorDataProfilingConfig | None = (
        Field(
            None,
            description="Databricks data profiling configuration for a Databricks Data Quality Monitor on this table. When set, Laktory automatically constructs a DataQualityMonitor using this sink's table as the monitored object.",
        )
    )

    @model_validator(mode="after")
    def validate_sink(self) -> Any:
        if self.dataframe_backend == DataFrameBackends.POLARS:
            raise ValueError(
                "Unity Catalog data sink does not support the Polars backend."
            )
        return self

    @property
    def data_quality_monitor(self) -> "DataQualityMonitor | None":
        """Build a DataQualityMonitor for this sink from the configured databricks_data_profiling_config."""
        if self.databricks_data_profiling_config is None:
            return None

        from laktory.models.resources.databricks.dataqualitymonitor import (
            DataQualityMonitor,
        )

        return DataQualityMonitor(
            object_type="table",
            object_id=self.full_name,
            data_profiling_config=self.databricks_data_profiling_config,
        )
