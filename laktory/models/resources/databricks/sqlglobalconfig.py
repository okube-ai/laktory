from laktory.models.resources.databricks.sqlglobalconfig_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.sqlglobalconfig_base import SqlGlobalConfigBase


class SqlGlobalConfig(SqlGlobalConfigBase):
    """
    Databricks SQL global configuration

    Workspace-wide configuration of SQL warehouses: data access properties,
    security policy, instance profile and SQL configuration parameters.
    Workspace singleton; changes restart running SQL warehouses.

    Examples
    --------
    ```py
    import io

    from laktory import models

    config_yaml = '''
    security_policy: DATA_ACCESS_CONTROL
    sql_config_params:
      ANSI_MODE: 'true'
    '''
    config = models.resources.databricks.SqlGlobalConfig.model_validate_yaml(
        io.StringIO(config_yaml)
    )
    ```

    References
    ----------

    * [Databricks SQL admin settings](https://docs.databricks.com/aws/en/admin/sql/)
    * [Terraform databricks_sql_global_config](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/sql_global_config)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
