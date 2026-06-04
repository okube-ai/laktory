from __future__ import annotations

import re
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Union

from laktory._logger import get_logger
from laktory.models.resources.databricks.dataqualitymonitor_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.dataqualitymonitor_base import (
    DataQualityMonitorBase,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = get_logger(__name__)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Mapping from human-readable granularity strings (old API) to SDK enum values
_GRANULARITY_MAP = {
    "5 minutes": "AGGREGATION_GRANULARITY_5_MINUTES",
    "30 minutes": "AGGREGATION_GRANULARITY_30_MINUTES",
    "1 hour": "AGGREGATION_GRANULARITY_1_HOUR",
    "1 day": "AGGREGATION_GRANULARITY_1_DAY",
    "1 week": "AGGREGATION_GRANULARITY_1_WEEK",
    "2 weeks": "AGGREGATION_GRANULARITY_2_WEEKS",
    "3 weeks": "AGGREGATION_GRANULARITY_3_WEEKS",
    "4 weeks": "AGGREGATION_GRANULARITY_4_WEEKS",
    "1 month": "AGGREGATION_GRANULARITY_1_MONTH",
    "1 year": "AGGREGATION_GRANULARITY_1_YEAR",
}


def _is_uuid(s: str) -> bool:
    return bool(_UUID_RE.match(s))


def _map_granularity(g: str) -> str:
    return _GRANULARITY_MAP.get(g, g)


# ----------------------------------------------------------------------- #
# SDK Client                                                              #
# ----------------------------------------------------------------------- #


class DataQualityMonitorSDKClient:
    def __init__(
        self,
        dqm: "DataQualityMonitor",
        workspace_client: Union["WorkspaceClient", None],
    ):
        self.dqm = dqm
        self._ws = workspace_client

    @cached_property
    def ws(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient

        if self._ws:
            return self._ws
        return WorkspaceClient()

    def _resolve_object_id(self) -> str:
        oid = self.dqm.object_id
        if _is_uuid(oid):
            return oid
        return self.ws.tables.get(oid).table_id

    def _resolve_output_schema_id(self) -> str:
        sid = self.dqm.data_profiling_config.output_schema_id
        if _is_uuid(sid):
            return sid
        return self.ws.schemas.get(sid).schema_id

    def _build_sdk_monitor(self, object_id: str, schema_id: str):
        from databricks.sdk.service.dataquality import AggregationGranularity
        from databricks.sdk.service.dataquality import CronSchedule
        from databricks.sdk.service.dataquality import DataProfilingConfig
        from databricks.sdk.service.dataquality import DataProfilingCustomMetricType
        from databricks.sdk.service.dataquality import InferenceLogConfig
        from databricks.sdk.service.dataquality import InferenceProblemType
        from databricks.sdk.service.dataquality import Monitor
        from databricks.sdk.service.dataquality import NotificationDestination
        from databricks.sdk.service.dataquality import NotificationSettings
        from databricks.sdk.service.dataquality import SnapshotConfig
        from databricks.sdk.service.dataquality import TimeSeriesConfig

        cfg = self.dqm.data_profiling_config

        dp_cfg = DataProfilingConfig(output_schema_id=schema_id)
        dp_cfg.assets_dir = cfg.assets_dir
        dp_cfg.baseline_table_name = cfg.baseline_table_name
        dp_cfg.skip_builtin_dashboard = cfg.skip_builtin_dashboard
        dp_cfg.slicing_exprs = cfg.slicing_exprs
        dp_cfg.warehouse_id = cfg.warehouse_id

        if cfg.custom_metrics is not None:
            from databricks.sdk.service.dataquality import DataProfilingCustomMetric

            dp_cfg.custom_metrics = [
                DataProfilingCustomMetric(
                    type=DataProfilingCustomMetricType(m.type),
                    name=m.name,
                    definition=m.definition,
                    input_columns=m.input_columns,
                    output_data_type=m.output_data_type,
                )
                for m in cfg.custom_metrics
            ]

        if cfg.snapshot is not None:
            dp_cfg.snapshot = SnapshotConfig()

        if cfg.time_series is not None:
            ts = cfg.time_series
            dp_cfg.time_series = TimeSeriesConfig(
                timestamp_column=ts.timestamp_column,
                granularities=[
                    AggregationGranularity(_map_granularity(g))
                    for g in ts.granularities
                ],
            )

        if cfg.inference_log is not None:
            il = cfg.inference_log
            dp_cfg.inference_log = InferenceLogConfig(
                problem_type=InferenceProblemType(il.problem_type),
                timestamp_column=il.timestamp_column,
                granularities=[
                    AggregationGranularity(_map_granularity(g))
                    for g in il.granularities
                ],
                prediction_column=il.prediction_column,
                model_id_column=il.model_id_column,
                label_column=il.label_column,
            )

        if cfg.schedule is not None:
            s = cfg.schedule
            dp_cfg.schedule = CronSchedule(
                quartz_cron_expression=s.quartz_cron_expression,
                timezone_id=s.timezone_id,
            )

        if cfg.notification_settings is not None:
            n = cfg.notification_settings
            on_failure = None
            if n.on_failure is not None:
                on_failure = NotificationDestination(
                    email_addresses=n.on_failure.email_addresses
                )
            dp_cfg.notification_settings = NotificationSettings(on_failure=on_failure)

        return Monitor(
            object_type=self.dqm.object_type.lower(),
            object_id=object_id,
            data_profiling_config=dp_cfg,
        )

    def get(self):
        from databricks.sdk.errors.platform import NotFound
        from databricks.sdk.errors.platform import ResourceDoesNotExist

        try:
            object_id = self._resolve_object_id()
            return self.ws.data_quality.get_monitor(
                self.dqm.object_type.lower(), object_id
            )
        except (NotFound, ResourceDoesNotExist):
            return None

    def create_or_update(self):
        existing = self.get()
        if existing:
            result = self.update(existing)
            if not result:
                self.delete()
                result = self.create()
        else:
            result = self.create()
        logger.info(f"Success: {result}")

    def create(self):
        object_id = self._resolve_object_id()
        schema_id = self._resolve_output_schema_id()
        monitor = self._build_sdk_monitor(object_id, schema_id)
        logger.info(f"Creating data quality monitor for {self.dqm.object_id}")
        return self.ws.data_quality.create_monitor(monitor)

    def update(self, existing):
        from databricks.sdk.errors.platform import ResourceConflict
        from databricks.sdk.service.dataquality import DataProfilingStatus

        existing_cfg = existing.data_profiling_config

        # A failed/errored monitor cannot be updated — delete and recreate
        if existing_cfg and existing_cfg.status in (
            DataProfilingStatus.DATA_PROFILING_STATUS_FAILED,
            DataProfilingStatus.DATA_PROFILING_STATUS_ERROR,
        ):
            logger.info(
                f"Data quality monitor for {self.dqm.object_id} is in state "
                f"{existing_cfg.status.value}. Deleting and recreating."
            )
            return False

        # Incompatible changes require delete + recreate
        if existing_cfg:
            if self.dqm.data_profiling_config.assets_dir != existing_cfg.assets_dir:
                return False
            existing_has_ts = existing_cfg.time_series is not None
            desired_has_ts = self.dqm.data_profiling_config.time_series is not None
            if existing_has_ts != desired_has_ts:
                return False

        object_id = self._resolve_object_id()
        schema_id = self._resolve_output_schema_id()
        monitor = self._build_sdk_monitor(object_id, schema_id)

        # Check if update is actually needed
        desired_dict = monitor.as_dict().get("data_profiling_config", {})
        existing_dict = existing_cfg.as_dict() if existing_cfg else {}
        update_needed = any(
            desired_dict.get(k) != existing_dict.get(k)
            for k in desired_dict
            if k not in ("assets_dir", "output_schema_id")
        )
        if not update_needed:
            logger.info(
                f"Data quality monitor for {self.dqm.object_id} is already up-to-date."
            )
            return existing

        logger.info(f"Updating data quality monitor for {self.dqm.object_id}")
        try:
            return self.ws.data_quality.update_monitor(
                object_type=self.dqm.object_type.lower(),
                object_id=object_id,
                monitor=monitor,
                update_mask="data_profiling_config",
            )
        except ResourceConflict as e:
            logger.warning(
                f"Update rejected due to conflicting monitor state ({e}). "
                f"Deleting and recreating."
            )
            return False

    def delete(self):
        from databricks.sdk.errors.platform import NotFound
        from databricks.sdk.errors.platform import ResourceDoesNotExist

        try:
            object_id = self._resolve_object_id()
        except (NotFound, ResourceDoesNotExist):
            return

        try:
            logger.info(f"Deleting data quality monitor for {self.dqm.object_id}")
            self.ws.data_quality.delete_monitor(self.dqm.object_type.lower(), object_id)
        except (NotFound, ResourceDoesNotExist):
            pass


# ----------------------------------------------------------------------- #
# Main Model                                                              #
# ----------------------------------------------------------------------- #


class DataQualityMonitor(DataQualityMonitorBase):
    """
    Databricks Data Quality Monitor

    Manages data quality monitoring on Unity Catalog objects. For table-level
    data profiling, set `object_type` to `"table"` and configure
    `data_profiling_config`. For schema-level anomaly detection, set
    `object_type` to `"schema"` and configure `anomaly_detection_config`.

    When attached to a `UnityCatalogDataSink` via the `data_profiling_config`
    field, `object_type` and `object_id` are populated automatically from the
    sink. The `object_id` and `output_schema_id` inside `data_profiling_config`
    accept either a UUID or a human-readable name (e.g. `dev.finance.slv_prices`
    or `dev.monitoring`); Laktory resolves names to UUIDs at execution time.

    Examples
    --------
    ```py
    import io

    from laktory import models

    dqm_yaml = '''
    object_type: table
    object_id: dev.finance.slv_stock_prices
    data_profiling_config:
      output_schema_id: dev.monitoring
      snapshot: {}
    '''
    dqm = models.resources.databricks.DataQualityMonitor.model_validate_yaml(
        io.StringIO(dqm_yaml)
    )
    ```

    References
    ----------

    * [Databricks Data Quality Monitoring](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring)
    * [Terraform databricks_data_quality_monitor](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/data_quality_monitor)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.object_id

    @property
    def additional_core_resources(self) -> list:
        return []

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["provider_config"]

    # ----------------------------------------------------------------------- #
    # SDK Client                                                              #
    # ----------------------------------------------------------------------- #

    def sdk(self, workspace_client: Union["WorkspaceClient", None] = None):
        return DataQualityMonitorSDKClient(self, workspace_client=workspace_client)
