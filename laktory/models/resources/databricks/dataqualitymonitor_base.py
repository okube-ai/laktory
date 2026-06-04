# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_data_quality_monitor
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class DataQualityMonitorAnomalyDetectionConfig(BaseModel):
    excluded_table_full_names: list[str] | None = Field(
        None,
        description="List of fully qualified table names to exclude from anomaly detection",
    )


class DataQualityMonitorDataProfilingConfigCustomMetrics(BaseModel):
    definition: str = Field(
        ...,
        description="Jinja template for a SQL expression that specifies how to compute the metric. See [create metric definition](https://docs.databricks.com/en/lakehouse-monitoring/custom-metrics.html#create-definition)",
    )
    input_columns: list[str] = Field(
        ...,
        description="A list of column names in the input table the metric should be computed for. Can use ``':table'`` to indicate that the metric needs information from multiple columns",
    )
    name: str = Field(..., description="Name of the metric in the output tables")
    output_data_type: str = Field(
        ..., description="The output type of the custom metric"
    )
    type: str = Field(
        ...,
        description="The type of the custom metric. Possible values are: `DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE`, `DATA_PROFILING_CUSTOM_METRIC_TYPE_DERIVED`, `DATA_PROFILING_CUSTOM_METRIC_TYPE_DRIFT`",
    )


class DataQualityMonitorDataProfilingConfigInferenceLog(BaseModel):
    granularities: list[str] = Field(
        ...,
        description="List of granularities to use when aggregating data into time windows based on their timestamp",
    )
    label_column: str | None = Field(None, description="Column for the label")
    model_id_column: str = Field(..., description="Column for the model identifier")
    prediction_column: str = Field(..., description="Column for the prediction")
    problem_type: str = Field(
        ...,
        description="Problem type the model aims to solve. Possible values are: `INFERENCE_PROBLEM_TYPE_CLASSIFICATION`, `INFERENCE_PROBLEM_TYPE_REGRESSION`",
    )
    timestamp_column: str = Field(..., description="Column for the timestamp")


class DataQualityMonitorDataProfilingConfigNotificationSettingsOnFailure(BaseModel):
    email_addresses: list[str] | None = Field(
        None,
        description="The list of email addresses to send the notification to. A maximum of 5 email addresses is supported",
    )


class DataQualityMonitorDataProfilingConfigNotificationSettings(BaseModel):
    on_failure: (
        DataQualityMonitorDataProfilingConfigNotificationSettingsOnFailure | None
    ) = Field(None, description="Destinations to send notifications on failure/timeout")


class DataQualityMonitorDataProfilingConfigSchedule(BaseModel):
    quartz_cron_expression: str = Field(
        ...,
        description="The expression that determines when to run the monitor. See [examples](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)",
    )
    timezone_id: str = Field(
        ...,
        description="A Java timezone id. The schedule for a job will be resolved with respect to this timezone. See `Java TimeZone <http://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html>`_ for details. The timezone id (e.g., ``America/Los_Angeles``) in which to evaluate the quartz expression",
    )


class DataQualityMonitorDataProfilingConfigSnapshot(BaseModel):
    pass


class DataQualityMonitorDataProfilingConfigTimeSeries(BaseModel):
    granularities: list[str] = Field(
        ...,
        description="List of granularities to use when aggregating data into time windows based on their timestamp",
    )
    timestamp_column: str = Field(..., description="Column for the timestamp")


class DataQualityMonitorDataProfilingConfig(BaseModel):
    assets_dir: str | None = Field(
        None,
        description="Field for specifying the absolute path to a custom directory to store data-monitoring assets. Normally prepopulated to a default user location via UI and Python APIs",
    )
    baseline_table_name: str | None = Field(
        None,
        description="Baseline table name. Baseline data is used to compute drift from the data in the monitored `table_name`. The baseline table and the monitored table shall have the same schema",
    )
    custom_metrics: list[DataQualityMonitorDataProfilingConfigCustomMetrics] | None = (
        Field(None, description="Custom metrics")
    )
    inference_log: DataQualityMonitorDataProfilingConfigInferenceLog | None = Field(
        None, description="`Analysis Configuration` for monitoring inference log tables"
    )
    notification_settings: (
        DataQualityMonitorDataProfilingConfigNotificationSettings | None
    ) = Field(None, description="Field for specifying notification settings")
    output_schema_id: str = Field(
        ..., description="ID of the schema where output tables are created"
    )
    schedule: DataQualityMonitorDataProfilingConfigSchedule | None = Field(
        None, description="The cron schedule"
    )
    skip_builtin_dashboard: bool | None = Field(
        None,
        description="Whether to skip creating a default dashboard summarizing data quality metrics",
    )
    slicing_exprs: list[str] | None = Field(
        None,
        description="List of column expressions to slice data with for targeted analysis. The data is grouped by each expression independently, resulting in a separate slice for each predicate and its complements. For example `slicing_exprs=[“col_1”, “col_2 > 10”]` will generate the following slices: two slices for `col_2 > 10` (True and False), and one slice per unique value in `col1`. For high-cardinality columns, only the top 100 unique values by frequency will generate slices",
    )
    snapshot: DataQualityMonitorDataProfilingConfigSnapshot | None = Field(
        None, description="`Analysis Configuration` for monitoring snapshot tables"
    )
    time_series: DataQualityMonitorDataProfilingConfigTimeSeries | None = Field(
        None, description="`Analysis Configuration` for monitoring time series tables"
    )
    warehouse_id: str | None = Field(
        None,
        description="Optional argument to specify the warehouse for dashboard creation. If not specified, the first running warehouse will be used",
    )


class DataQualityMonitorProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class DataQualityMonitorBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_data_quality_monitor`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    object_id: str = Field(
        ...,
        description="The UUID of the request object. It is `schema_id` for `schema`, and `table_id` for `table`",
    )
    object_type: str = Field(
        ...,
        description="The type of the monitored object. Can be one of the following: `schema` or `table`",
    )
    anomaly_detection_config: DataQualityMonitorAnomalyDetectionConfig | None = Field(
        None,
        description="Anomaly Detection Configuration, applicable to `schema` object types",
    )
    data_profiling_config: DataQualityMonitorDataProfilingConfig | None = Field(
        None,
        description="Data Profiling Configuration, applicable to `table` object types. Exactly one `Analysis Configuration` must be present",
    )
    provider_config: DataQualityMonitorProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_data_quality_monitor"


__all__ = [
    "DataQualityMonitorAnomalyDetectionConfig",
    "DataQualityMonitorDataProfilingConfig",
    "DataQualityMonitorDataProfilingConfigCustomMetrics",
    "DataQualityMonitorDataProfilingConfigInferenceLog",
    "DataQualityMonitorDataProfilingConfigNotificationSettings",
    "DataQualityMonitorDataProfilingConfigNotificationSettingsOnFailure",
    "DataQualityMonitorDataProfilingConfigSchedule",
    "DataQualityMonitorDataProfilingConfigSnapshot",
    "DataQualityMonitorDataProfilingConfigTimeSeries",
    "DataQualityMonitorProviderConfig",
    "DataQualityMonitorBase",
]
