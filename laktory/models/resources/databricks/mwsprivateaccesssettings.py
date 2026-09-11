from laktory.models.resources.databricks.mwsprivateaccesssettings_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsprivateaccesssettings_base import (
    MwsPrivateAccessSettingsBase,
)


class MwsPrivateAccessSettings(MwsPrivateAccessSettingsBase):
    """
    Databricks Mws Private Access Settings

    Examples
    --------
    ```py
    import io

    from laktory import models

    settings_yaml = '''
    private_access_settings_name: my-private-access-settings
    region: us-east-1
    '''
    settings = models.resources.databricks.MwsPrivateAccessSettings.model_validate_yaml(
        io.StringIO(settings_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Private Access Settings](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_private_access_settings)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.private_access_settings_name
