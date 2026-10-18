from laktory.models.resources.databricks.defaultnamespacesetting_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.defaultnamespacesetting_base import (
    DefaultNamespaceSettingBase,
)


class DefaultNamespaceSetting(DefaultNamespaceSettingBase):
    """
    Databricks default namespace setting

    Default catalog used by queries that don't reference a catalog (workspace
    singleton). Requires a restart of running clusters and SQL warehouses.

    Examples
    --------
    ```py
    import io

    from laktory import models

    setting_yaml = '''
    namespace:
      value: dev
    '''
    setting = models.resources.databricks.DefaultNamespaceSetting.model_validate_yaml(
        io.StringIO(setting_yaml)
    )
    ```

    References
    ----------

    * [Databricks default catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/hive-metastore#default-catalog)
    * [Terraform databricks_default_namespace_setting](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/default_namespace_setting)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
