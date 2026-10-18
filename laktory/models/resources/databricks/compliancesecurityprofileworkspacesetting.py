from laktory.models.resources.databricks.compliancesecurityprofileworkspacesetting_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.compliancesecurityprofileworkspacesetting_base import (
    ComplianceSecurityProfileWorkspaceSettingBase,
)


class ComplianceSecurityProfileWorkspaceSetting(
    ComplianceSecurityProfileWorkspaceSettingBase
):
    """
    Databricks compliance security profile setting

    Enables the compliance security profile and compliance standards (e.g.
    HIPAA, PCI-DSS) on the workspace. Workspace singleton; can't be disabled
    once enabled.

    Examples
    --------
    ```py
    import io

    from laktory import models

    setting_yaml = '''
    compliance_security_profile_workspace:
      is_enabled: true
      compliance_standards:
      - HIPAA
    '''
    setting = models.resources.databricks.ComplianceSecurityProfileWorkspaceSetting.model_validate_yaml(
        io.StringIO(setting_yaml)
    )
    ```

    References
    ----------

    * [Databricks compliance security profile](https://docs.databricks.com/aws/en/security/privacy/security-profile)
    * [Terraform databricks_compliance_security_profile_setting](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/compliance_security_profile_setting)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
