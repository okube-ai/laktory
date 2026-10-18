from laktory.models.resources.databricks.restrictworkspaceadminssetting_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.restrictworkspaceadminssetting_base import (
    RestrictWorkspaceAdminsSettingBase,
)


class RestrictWorkspaceAdminsSetting(RestrictWorkspaceAdminsSettingBase):
    """
    Databricks restrict workspace admins setting

    Restricts the capabilities of workspace admins (e.g. creating tokens on
    behalf of service principals, changing a job owner or run as). Workspace
    singleton.

    Examples
    --------
    ```py
    import io

    from laktory import models

    setting_yaml = '''
    restrict_workspace_admins:
      status: RESTRICT_TOKENS_AND_JOB_RUN_AS
    '''
    setting = (
        models.resources.databricks.RestrictWorkspaceAdminsSetting.model_validate_yaml(
            io.StringIO(setting_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks restrict workspace admins](https://docs.databricks.com/aws/en/admin/workspace-settings/restrict-workspace-admins)
    * [Terraform databricks_restrict_workspace_admins_setting](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/restrict_workspace_admins_setting)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
