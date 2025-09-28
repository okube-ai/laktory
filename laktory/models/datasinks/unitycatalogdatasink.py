from typing import Any
from typing import Literal

from narwhals import LazyFrame
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.resources.databricks.qualitymonitor import QualityMonitor

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
    databricks_quality_monitor: QualityMonitor | None = Field(
        None,
        description="Databricks Quality Monitor. Requires `databricks_quality_monitor_enabled` set to `True`.",
    )

    @model_validator(mode="after")
    def validate_qm(self) -> Any:
        if self.databricks_quality_monitor is not None:
            if (
                self.parent_pipeline
                and not self.parent_pipeline.databricks_quality_monitor_enabled
            ):
                raise ValueError(
                    "`databricks_quality_monitor_enabled` must be set to `True` to use quality monitor on Unity Catalog data sinks."
                )
        return self

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
