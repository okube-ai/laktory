from laktory.models.resources.databricks.enhancedsecuritymonitoringworkspacesetting_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.enhancedsecuritymonitoringworkspacesetting_base import (
    EnhancedSecurityMonitoringWorkspaceSettingBase,
)


class EnhancedSecurityMonitoringWorkspaceSetting(
    EnhancedSecurityMonitoringWorkspaceSettingBase
):
    """
    Databricks enhanced security monitoring setting

    Enables enhanced security monitoring (hardened images and security
    agents) on the workspace compute. Workspace singleton.

    Examples
    --------
    ```py
    import io

    from laktory import models

    setting_yaml = '''
    enhanced_security_monitoring_workspace:
      is_enabled: true
    '''
    setting = models.resources.databricks.EnhancedSecurityMonitoringWorkspaceSetting.model_validate_yaml(
        io.StringIO(setting_yaml)
    )
    ```

    References
    ----------

    * [Databricks enhanced security monitoring](https://docs.databricks.com/aws/en/security/privacy/enhanced-security-monitoring)
    * [Terraform databricks_enhanced_security_monitoring_setting](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/enhanced_security_monitoring_setting)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
