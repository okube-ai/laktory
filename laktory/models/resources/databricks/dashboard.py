from pathlib import Path
from typing import Any
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field
from pydantic import model_validator

from laktory._settings import settings
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.dashboard_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.dashboard_base import DashboardBase
from laktory.models.resources.databricks.permissions import Permissions


class Dashboard(DashboardBase):
    """
    Databricks Lakeview Dashboard

    Examples
    --------
    ```py
    import io

    from laktory import models

    # Define job
    job_yaml = '''
    display_name: databricks-costs
    file_path: ./dashboards/databricks_costs.json
    parent_path: /.laktory/dashboards
    warehouse_id: a7d9f2kl8mp3q6rt
    access_controls:
        - group_name: account users
          permission_level: CAN_READ
        - group_name: account users
          permission_level: CAN_RUN
    '''
    dashboard = models.resources.databricks.Dashboard.model_validate_yaml(
        io.StringIO(job_yaml)
    )
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    name_prefix: str = Field(
        None, description="Prefix added to the dashboard display name"
    )
    name_suffix: str = Field(
        None, description="Suffix added to the dashboard display name"
    )
    parent_path_: str | None = Field(
        None,
        description="""
        The path to a workspace folder (inside laktory root) containing the dashboard. If changed, the query will be
        recreated.
        """,
        validation_alias=AliasChoices("parent_path", "dirpath", "parent_path_"),
        exclude=True,
    )

    @computed_field(description="parent_path")
    @property
    def parent_path(self) -> str:
        if self.parent_path_ is None:
            self.parent_path_ = ""
        if self.parent_path_.startswith("/"):
            self.parent_path_ = self.parent_path_[1:]

        parent_path = Path(settings.workspace_root) / self.parent_path_
        return parent_path.as_posix()

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.display_name = self.name_prefix + self.display_name
                self.name_prefix = ""
            if self.name_suffix:
                self.display_name = self.display_name + self.name_suffix
                self.name_suffix = ""
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name

    @property
    def additional_core_resources(self) -> list:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    dashboard_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "access_controls",
            "name_prefix",
            "name_suffix",
        ]
