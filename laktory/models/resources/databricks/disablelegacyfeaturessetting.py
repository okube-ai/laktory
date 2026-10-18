from laktory.models.resources.databricks.disablelegacyfeaturessetting_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.disablelegacyfeaturessetting_base import (
    DisableLegacyFeaturesSettingBase,
)


class DisableLegacyFeaturesSetting(DisableLegacyFeaturesSettingBase):
    """
    Databricks disable legacy features setting (account level)

    Disables legacy features (DBFS root and mounts, Hive metastore, No-isolation
    shared compute, Databricks Runtime < 13.3) on newly created workspaces of
    the account. Account singleton, requires an account-level provider.

    Examples
    --------
    ```py
    import io

    from laktory import models

    setting_yaml = '''
    disable_legacy_features:
      value: true
    '''
    setting = (
        models.resources.databricks.DisableLegacyFeaturesSetting.model_validate_yaml(
            io.StringIO(setting_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks legacy features](https://docs.databricks.com/aws/en/admin/account-settings/legacy-features)
    * [Terraform databricks_disable_legacy_features_setting](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/disable_legacy_features_setting)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
