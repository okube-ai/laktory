from laktory.models.resources.databricks.dataqualitymonitor_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.dataqualitymonitor_base import (
    DataQualityMonitorBase,
)


class DataQualityMonitor(DataQualityMonitorBase):
    """
    Databricks Data Quality Monitor

    Examples
    --------
    ```py
    import io

    from laktory import models

    dqm_yaml = '''
    object_id: dev.finance.slv_stock_prices
    object_type: TABLE
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

    * [Databricks Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html)
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
