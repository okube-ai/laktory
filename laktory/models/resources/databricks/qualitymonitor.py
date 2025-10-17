import time
from dataclasses import fields
from dataclasses import is_dataclass
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Mapping
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = get_logger(__name__)

# ----------------------------------------------------------------------- #
# SDK Client                                                              #
# ----------------------------------------------------------------------- #


def from_dict(cls, data: dict):
    import databricks.sdk.service.catalog as catalog

    if not is_dataclass(cls):
        return data
    kwargs = {}
    for f in fields(cls):
        if f.name not in data:
            continue
        value = data[f.name]
        cls_str = f.type.replace("Optional[", "").replace("]", "")
        _cls = getattr(catalog, cls_str, None)
        if _cls:
            if isinstance(value, Mapping):
                return from_dict(_cls, value)
            return value

        kwargs[f.name] = value
    return cls(**kwargs)


class QualityMonitorSDKClient:
    def __init__(
        self, quality_monitor_resource, workspace_client: Union["WorkspaceClient", None]
    ):
        self.qmr = quality_monitor_resource
        self._ws = workspace_client

    @cached_property
    def ws(self):
        from databricks.sdk import WorkspaceClient

        if self._ws:
            return self._ws
        return WorkspaceClient()

    @property
    def table_name(self):
        table_name = self.qmr.table_name
        if table_name is None:
            raise ValueError("`TableDataSink` or `table_name` has not been set")
        return table_name

    def get(self):
        from databricks.sdk.errors.platform import ResourceDoesNotExist

        try:
            return self.ws.quality_monitors.get(self.table_name)
        except ResourceDoesNotExist:
            return None

    def create_or_update(self):
        _qm = self.get()
        if _qm:
            _qm = self.update(_qm)
            if not _qm:
                self.delete()
                _qm = self.create()
        else:
            _qm = self.create()

        logger.info(f"Success: {_qm}")

    def create(self):
        """
        Bypass ws.quality_monitors.create to avoid having instantiating the data
        classes.
        """
        from databricks.sdk.service.catalog import MonitorInfo

        body = self.qmr.model_dump(exclude_unset=True)

        logger.info(f"Creating Quality Monitor for {self.table_name}")

        table_name = body.pop("table_name")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        res = self.ws.quality_monitors._api.do(
            "POST",
            f"/api/2.1/unity-catalog/tables/{table_name}/monitor",
            body=body,
            headers=headers,
        )
        return MonitorInfo.from_dict(res)

    def update(self, _qm):
        """
        Bypass ws.quality_monitors.update to avoid having instantiating the data
        classes.
        """
        from databricks.sdk.service.catalog import MonitorInfo
        from databricks.sdk.service.catalog import MonitorInfoStatus

        body = self.qmr.model_dump(exclude_unset=True, exclude_none=True)

        # Exit if update is not possible
        if self.qmr.assets_dir != _qm.assets_dir:
            return False
        if self.qmr.time_series is None and _qm.time_series is not None:
            return False
        if self.qmr.time_series is not None and _qm.time_series is None:
            return False
        table_name = body.pop("table_name")
        body.pop("warehouse_id", None)
        body.pop("assets_dir", None)

        # Wait for previous update or creation to be completed
        while _qm.status == MonitorInfoStatus.MONITOR_STATUS_PENDING:
            time.sleep(1.0)
            _qm = self.get()

        # Get current state
        body0 = _qm.as_dict()

        update_required = False
        for k, v in body.items():
            v0 = body0.get(k, None)
            if v != v0:
                update_required = True
                break

        if not update_required:
            logger.info(f"Quality Monitor for {self.table_name} is already up-to-date.")
            return _qm

        logger.info(f"Updating Quality Monitor for {self.table_name} with body {body}.")

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        res = self.ws.quality_monitors._api.do(
            "PUT",
            f"/api/2.1/unity-catalog/tables/{table_name}/monitor",
            body=body,
            headers=headers,
        )
        return MonitorInfo.from_dict(res)

    def delete(self):
        from databricks.sdk.errors.platform import NotFound
        from databricks.sdk.errors.platform import ResourceDoesNotExist

        _qm = self.get()
        if _qm is None:
            return

        table_full_name = self.table_name

        logger.info(f"Deleting Quality Monitor for {table_full_name}")
        self.ws.quality_monitors.delete(table_name=table_full_name)

        for _table_name in [
            _qm.drift_metrics_table_name,
            _qm.profile_metrics_table_name,
        ]:
            try:
                logger.info(f"Deleting metrics table {_table_name}")
                self.ws.tables.delete(_table_name)
            except NotFound:
                pass

        logger.info(f"Deleting monitor assets directory {_qm.assets_dir}")
        try:
            self.ws.workspace.delete(_qm.assets_dir, recursive=True)
        except ResourceDoesNotExist:
            pass


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
        None, description="Who to send notifications to on monitor failure."
    )
    on_new_classification_tag_detected: QualityMonitorNotificationsOnNewClassificationTagDetected = Field(
        None,
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
    pass


class QualityMonitorSchedule(BaseModel):
    quartz_cron_expression: list[str] = Field(
        ...,
        description="string expression that determines when to run the monitor. See Quartz documentation for examples.",
    )
    timezone_id: str = Field(
        ...,
        description="string with timezone id (e.g., PST) in which to evaluate the Quartz expression.",
    )


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
        snapshot={},
    )
    ```
    """

    assets_dir: str = Field(
        ...,
        description="The directory to store the monitoring assets (Eg. Dashboard and Metric Tables)",
    )

    output_schema_name_: str = Field(
        None,
        description="""
        Schema where output metric tables are created. Its of the format {catalog}.{schema}.
        """,
        validation_alias=AliasChoices("output_schema_name", "output_schema_name_"),
        exclude=True,
    )
    table_name_: str = Field(
        None,
        description="""
        The full name of the table to attach the monitor too. Its of the format {catalog}.{schema}.{tableName}
        """,
        validation_alias=AliasChoices("table_name", "table_name_"),
        exclude=True,
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
    notifications: QualityMonitorNotifications = Field(
        None, description="The notification settings for the monitor."
    )
    schedule: QualityMonitorSchedule | None = Field(
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
    _table: Any = None

    @model_validator(mode="after")
    def validate_type(self) -> Any:
        if self.snapshot is None and self.time_series is None:
            raise ValueError("Either `snapshot` or `time_series` must be configured.")
        if self.snapshot is not None and self.time_series is not None:
            raise ValueError(
                "Only one of `snapshot` or `time_series` must be configured."
            )
        return self

    @computed_field(description="table_name")
    @property
    def table_name(self) -> str | None:
        """Remove backticks from table name as they are not accepted by the API"""
        if self.table_name_:
            return self.table_name_.replace("`", "")

        if self._table:
            return self._table.full_name.replace("`", "")

        return None

    @computed_field(description="table_name")
    @property
    def output_schema_name(self) -> str | None:
        if self.output_schema_name_:
            return self.output_schema_name_

        if self._table:
            return self._table.catalog_name + "." + self._table.schema_name

        return None

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
        return ["_table"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_quality_monitor"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    # ----------------------------------------------------------------------- #
    # SDK Client                                                              #
    # ----------------------------------------------------------------------- #

    def sdk(self, workspace_client: Union["WorkspaceClient", None] = None):
        return QualityMonitorSDKClient(self, workspace_client=workspace_client)
