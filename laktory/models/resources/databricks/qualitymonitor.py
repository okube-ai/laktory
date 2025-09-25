from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class QualityMonitorCustomMetric(BaseModel):
    definition: str = Field(..., description="Create metric definition")
    input_columns: list[str] = Field(
        ...,
        description="Columns on the monitored table to apply the custom metrics to.",
    )
    name: str = Field(..., description="Name of the custom metric.")
    output_data_type: str = Field(
        ..., description="The output type of the custom metric."
    )
    type: str = Field(..., description="The type of the custom metric.")


class QualityMonitorDataClassificationConfig(BaseModel):
    enabled: bool = Field(..., description="")


class QualityMonitorInferenceLog(BaseModel):
    granularities: list[str] = Field(
        ...,
        description="List of granularities to use when aggregating data into time windows based on their timestamp.",
    )
    model_id_col: str = Field(..., description="Column of the model id or version")
    prediction_col: str = Field(..., description="Column of the model prediction")
    problem_type: str = Field(
        ...,
        description="Problem type the model aims to solve. Either PROBLEM_TYPE_CLASSIFICATION or PROBLEM_TYPE_REGRESSION",
    )
    timestamp_col: str = Field(
        ..., description="Column of the timestamp of predictions"
    )
    label_col: str = Field(None, description="Column of the model label")
    prediction_proba_col: str = Field(
        None, description="Column of the model prediction probabilities"
    )


class QualityMonitorNotificationsOnFailure(BaseModel):
    email_addresses: list[str] = Field(..., description="")


class QualityMonitorNotificationsOnNewClassificationTagDetected(BaseModel):
    email_addresses: list[str] = Field(..., description="")


class QualityMonitorNotifications(BaseModel):
    on_failure: QualityMonitorNotificationsOnFailure = Field(
        ..., description="Who to send notifications to on monitor failure."
    )
    on_new_classification_tag_detected: QualityMonitorNotificationsOnNewClassificationTagDetected = Field(
        ...,
        description="Who to send notifications to when new data classification tags are detected.",
    )


class QualityMonitorTimeSeries(BaseModel):
    granularities: list[str] = Field(
        ...,
        description="List of granularities to use when aggregating data into time windows based on their timestamp.",
    )
    timestamp_col: str = Field(
        ..., description="Column of the timestamp of predictions."
    )


class QualityMonitorSnapshot(BaseModel):
    granularities: list[str] = Field(
        ...,
        description="List of granularities to use when aggregating data into time windows based on their timestamp.",
    )


class QualityMonitorSchedule(BaseModel):
    quartz_cron_expression: list[str] = Field(
        ...,
        description="string expression that determines when to run the monitor. See Quartz documentation for examples.",
    )
    timezone_id: str = Field(
        ...,
        description="string with timezone id (e.g., PST) in which to evaluate the Quartz expression.",
    )
    pause_status: str = Field(None, description="")


class QualityMonitor(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Quality Monitor

    Examples
    --------
    ```py
    import laktory as lk

    qm = lk.models.resources.databricks.QualityMonitor(
        assets_dir="/.laktory/qualitymonitors",
        output_schema_name="dev.monitoring",
        table_name="dev.slv_stock_prices",
    )
    ```
    """

    assets_dir: str = Field(
        ...,
        description="The directory to store the monitoring assets (Eg. Dashboard and Metric Tables)",
    )
    output_schema_name: str = Field(
        ..., description="Schema where output metric tables are created"
    )
    table_name: str = Field(
        ...,
        description="The full name of the table to attach the monitor too. Its of the format {catalog}.{schema}.{tableName}",
    )
    baseline_table_name: str = Field(
        None,
        description="Name of the baseline table from which drift metrics are computed from.Columns in the monitored table should also be present in the baseline table.",
    )
    custom_metrics: list[QualityMonitorCustomMetric] = Field(
        None,
        description="Custom metrics to compute on the monitored table. These can be aggregate metrics, derived metrics (from already computed aggregate metrics), or drift metrics (comparing metrics across time windows).",
    )
    data_classification_config: QualityMonitorDataClassificationConfig = Field(
        None, description="The data classification config for the monitor"
    )
    inference_log: QualityMonitorInferenceLog = Field(
        None, description="Configuration for the inference log monitor"
    )
    latest_monitor_failure_msg: str = Field(None, description="")
    monitor_id: str = Field(
        None,
        description="ID of this monitor is the same as the full table name of the format {catalog}.{schema_name}.{table_name}",
    )
    notifications: list[QualityMonitorNotifications] = Field(
        None, description="The notification settings for the monitor."
    )
    schedule: QualityMonitorSchedule = Field(
        None,
        description="The schedule for automatically updating and refreshing metric tables.",
    )
    skip_builtin_dashboard: bool = Field(
        None,
        description="Whether to skip creating a default dashboard summarizing data quality metrics. (Can't be updated after creation).",
    )
    slicing_exprs: list[str] = Field(
        None,
        description="List of column expressions to slice data with for targeted analysis. The data is grouped by each expression independently, resulting in a separate slice for each predicate and its complements. For high-cardinality columns, only the top 100 unique values by frequency will generate slices.",
    )
    snapshot: QualityMonitorSnapshot = Field(
        None, description="Configuration for monitoring snapshot tables."
    )
    time_series: QualityMonitorTimeSeries = Field(
        None, description="Configuration for monitoring timeseries tables."
    )
    warehouse_id: str = Field(
        None,
        description="Optional argument to specify the warehouse for dashboard creation. If not specified, the first running warehouse will be used. (Can't be updated after creation)",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """ """
        resources = []
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:QualityMonitor"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return []

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_quality_monitor"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
