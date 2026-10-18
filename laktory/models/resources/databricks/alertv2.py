from pathlib import Path
from typing import Any

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field
from pydantic import model_validator

from laktory._settings import settings
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.alertv2_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.alertv2_base import AlertV2Base
from laktory.models.resources.databricks.permissions import Permissions


class AlertV2(AlertV2Base):
    """
    Databricks SQL Alert (v2)

    Scheduled SQL alert evaluating its own query text, with notifications sent
    when a condition is met. Latest version of the SQL alerts API; `Alert`
    manages alerts of the previous API.

    Examples
    --------
    ```py
    import io

    from laktory import models

    alert_yaml = '''
    display_name: high-stock-price
    parent_path: /alerts
    warehouse_id: a7d9f2kl8mp3q6rt
    query_text: SELECT MAX(close) AS max_close FROM dev.finance.slv_stock_prices
    evaluation:
      comparison_operator: GREATER_THAN
      source:
        name: max_close
      threshold:
        value:
          double_value: 500.0
      notification:
        subscriptions:
        - user_email: finance-alerts@okube.ai
    schedule:
      quartz_cron_schedule: 0 0 8 * * ?
      timezone_id: America/Montreal
    access_controls:
    - group_name: account users
      permission_level: CAN_READ
    '''
    alert = models.resources.databricks.AlertV2.model_validate_yaml(
        io.StringIO(alert_yaml)
    )
    print(alert.parent_path)
    # > /Workspace/.laktory/alerts
    ```

    References
    ----------

    * [Databricks SQL Alerts](https://docs.databricks.com/aws/en/sql/user/alerts/)
    * [Terraform databricks_alert_v2](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/alert_v2)
    """

    access_controls: list[AccessControl] = Field(
        [], description="SQL Alert access controls"
    )
    name_prefix: str = Field(None, description="Prefix added to the alert display name")
    name_suffix: str = Field(None, description="Suffix added to the alert display name")
    parent_path_: str | None = Field(
        None,
        description="""
        The path to a workspace folder (inside laktory root) containing the alert. If changed, the alert will be
        recreated. The folder must exist, e.g. created by a `Directory` or a workspace file of the stack.
        """,
        validation_alias=AliasChoices("parent_path", "dirpath", "parent_path_"),
        exclude=True,
    )

    @computed_field(description="parent_path")
    @property
    def parent_path(self) -> str:
        if self.parent_path_ is None:
            self.parent_path_ = ""
        self.parent_path_ = self.parent_path_.removeprefix("/")
        parent_path = Path(settings.workspace_root) / self.parent_path_
        parent_path = parent_path.as_posix()
        # The API returns the path with the `/Workspace` prefix: set it to avoid an
        # inconsistent result after apply
        if not parent_path.startswith("/Workspace/"):
            parent_path = "/Workspace" + parent_path
        return parent_path

    @model_validator(mode="after")
    def update_name(self) -> Any:
        if self.name_prefix:
            self._setattr("display_name", self.name_prefix + self.display_name)
            self._setattr("name_prefix", "")
        if self.name_suffix:
            self._setattr("display_name", self.display_name + self.name_suffix)
            self._setattr("name_suffix", "")
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name.replace(" ", "_")

    @property
    def additional_core_resources(self) -> list:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_options={"name": f"permissions-{self.resource_name}"},
                    access_controls=self.access_controls,
                    alert_v2_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["access_controls", "dirpath", "name_prefix", "name_suffix"]
