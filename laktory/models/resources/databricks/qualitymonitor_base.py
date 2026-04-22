# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_quality_monitor
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class QualityMonitorCustomMetrics(BaseModel):
    definition: str = Field(
        ...,
        description="[create metric definition](https://docs.databricks.com/en/lakehouse-monitoring/custom-metrics.html#create-definition)",
    )
    input_columns: list[str] = Field(
        ..., description="Columns on the monitored table to apply the custom metrics to"
    )
    name: str = Field(..., description="Name of the custom metric")
    output_data_type: str = Field(
        ..., description="The output type of the custom metric"
    )
    type: str = Field(..., description="The type of the custom metric")


class QualityMonitorBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_quality_monitor`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    assets_dir: str = Field(
        ...,
        description="- The directory to store the monitoring assets (Eg. Dashboard and Metric Tables)",
    )
    baseline_table_name: str | None = Field(
        None,
        description="Name of the baseline table from which drift metrics are computed from.Columns in the monitored table should also be present in the baseline table",
    )
    latest_monitor_failure_msg: str | None = Field(None)
    skip_builtin_dashboard: bool | None = Field(
        None,
        description="Whether to skip creating a default dashboard summarizing data quality metrics.  (Can't be updated after creation)",
    )
    slicing_exprs: list[str] | None = PluralField(
        None,
        plural="slicing_exprss",
        description="List of column expressions to slice data with for targeted analysis. The data is grouped by each expression independently, resulting in a separate slice for each predicate and its complements. For high-cardinality columns, only the top 100 unique values by frequency will generate slices",
    )
    warehouse_id: str | None = Field(
        None,
        description="Optional argument to specify the warehouse for dashboard creation. If not specified, the first running warehouse will be used.  (Can't be updated after creation)",
    )
    custom_metrics: list[QualityMonitorCustomMetrics] | None = PluralField(
        None,
        plural="custom_metricss",
        description="Custom metrics to compute on the monitored table. These can be aggregate metrics, derived metrics (from already computed aggregate metrics), or drift metrics (comparing metrics across time windows)",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_quality_monitor"
