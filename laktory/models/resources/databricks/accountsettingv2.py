from pydantic import Field

from laktory.models.resources.databricks._settingv2mixin import SettingV2Mixin
from laktory.models.resources.databricks.accountsettingv2_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.accountsettingv2_base import (
    AccountSettingV2Base,
)


class AccountSettingV2(SettingV2Mixin, AccountSettingV2Base):
    """
    Databricks account setting (settings v2)

    One account setting, identified by its `name`, with a value matching its
    type: `boolean_val`, `string_val`, `integer_val` or a typed value
    (e.g. `workspace_label`, `automatic_cluster_update_workspace`,
    `personal_compute`). The settings and their types are listed by the
    settings metadata API (`GET /api/2.1/settings-metadata`). Removing the
    resource doesn't reset the setting: the provider only drops it from the
    Terraform state, leaving the last value. Apply the default value before
    removing it to revert a change.
    Requires an account-level provider.

    Examples
    --------
    ```py
    import io

    from laktory import models

    setting_yaml = '''
    name: llm_proxy_partner_powered
    boolean_val:
      value: false
    '''
    setting = models.resources.databricks.AccountSettingV2.model_validate_yaml(
        io.StringIO(setting_yaml)
    )
    ```

    References
    ----------

    * [Databricks settings API](https://docs.databricks.com/api/workspace/settingsv2)
    * [Terraform databricks_account_setting_v2](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/account_setting_v2)
    """

    # Required (base marks it optional)
    name: str = Field(..., description="Name of the setting")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.name
