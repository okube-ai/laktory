# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_data_quality_monitor
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class DataQualityMonitorAnomalyDetectionConfig(BaseModel):
    excluded_table_full_names: list[str] | None = Field(None)


class DataQualityMonitorDataProfilingConfigCustomMetrics(BaseModel):
    definition: str = Field(...)
    input_columns: list[str] = Field(...)
    name: str = Field(...)
    output_data_type: str = Field(...)
    type: str = Field(...)


class DataQualityMonitorDataProfilingConfigInferenceLog(BaseModel):
    granularities: list[str] = Field(...)
    label_column: str | None = Field(None)
    model_id_column: str = Field(...)
    prediction_column: str = Field(...)
    problem_type: str = Field(...)
    timestamp_column: str = Field(...)


class DataQualityMonitorDataProfilingConfigNotificationSettingsOnFailure(BaseModel):
    email_addresses: list[str] | None = Field(None)


class DataQualityMonitorDataProfilingConfigNotificationSettings(BaseModel):
    on_failure: (
        DataQualityMonitorDataProfilingConfigNotificationSettingsOnFailure | None
    ) = Field(None)


class DataQualityMonitorDataProfilingConfigSchedule(BaseModel):
    quartz_cron_expression: str = Field(...)
    timezone_id: str = Field(...)


class DataQualityMonitorDataProfilingConfigSnapshot(BaseModel):
    pass


class DataQualityMonitorDataProfilingConfigTimeSeries(BaseModel):
    granularities: list[str] = Field(...)
    timestamp_column: str = Field(...)


class DataQualityMonitorDataProfilingConfig(BaseModel):
    assets_dir: str | None = Field(None)
    baseline_table_name: str | None = Field(None)
    custom_metrics: list[DataQualityMonitorDataProfilingConfigCustomMetrics] | None = (
        Field(None)
    )
    inference_log: DataQualityMonitorDataProfilingConfigInferenceLog | None = Field(
        None
    )
    notification_settings: (
        DataQualityMonitorDataProfilingConfigNotificationSettings | None
    ) = Field(None)
    output_schema_id: str = Field(...)
    schedule: DataQualityMonitorDataProfilingConfigSchedule | None = Field(None)
    skip_builtin_dashboard: bool | None = Field(None)
    slicing_exprs: list[str] | None = Field(None)
    snapshot: DataQualityMonitorDataProfilingConfigSnapshot | None = Field(None)
    time_series: DataQualityMonitorDataProfilingConfigTimeSeries | None = Field(None)
    warehouse_id: str | None = Field(None)


class DataQualityMonitorProviderConfig(BaseModel):
    workspace_id: str | None = Field(None)


class DataQualityMonitorBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_data_quality_monitor`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    object_id: str = Field(...)
    object_type: str = Field(...)
    anomaly_detection_config: DataQualityMonitorAnomalyDetectionConfig | None = Field(
        None
    )
    data_profiling_config: DataQualityMonitorDataProfilingConfig | None = Field(None)
    provider_config: DataQualityMonitorProviderConfig | None = Field(None)

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
