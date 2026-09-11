from laktory.models.resources.databricks.mwslogdelivery_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwslogdelivery_base import MwsLogDeliveryBase


class MwsLogDelivery(MwsLogDeliveryBase):
    """
    Databricks Mws Log Delivery

    Examples
    --------
    ```py
    import io

    from laktory import models

    delivery_yaml = '''
    account_id: ${vars.databricks_account_id}
    config_name: my-audit-log-delivery
    credentials_id: ${resources.mws-credentials-audit-logs.credentials_id}
    log_type: AUDIT_LOGS
    output_format: JSON
    storage_configuration_id: ${resources.mws-storage-configurations-audit-logs.storage_configuration_id}
    '''
    delivery = models.resources.databricks.MwsLogDelivery.model_validate_yaml(
        io.StringIO(delivery_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Log Delivery](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_log_delivery)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.config_name or self.log_type
