from laktory.models.resources.databricks.mwsstorageconfigurations_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsstorageconfigurations_base import (
    MwsStorageConfigurationsBase,
)


class MwsStorageConfigurations(MwsStorageConfigurationsBase):
    """
    Databricks Mws Storage Configurations

    Examples
    --------
    ```py
    import io

    from laktory import models

    storage_yaml = '''
    account_id: ${vars.databricks_account_id}
    bucket_name: my-root-bucket
    storage_configuration_name: my-storage-configuration
    '''
    storage = models.resources.databricks.MwsStorageConfigurations.model_validate_yaml(
        io.StringIO(storage_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Storage Configurations](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_storage_configurations)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.storage_configuration_name
