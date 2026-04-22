from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.app_base import AppBase
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource


class App(AppBase, PulumiResource):
    """
    Databricks App

    Examples
    --------
    ```py
    import io

    from laktory import models

    # Define App
    job_yaml = '''
    name: stocks-dash
    description: A dashboard app for visualizing stock prices.
    resources:
    - name: sql-warehouse
      sql_warehouse:
        id: warehouse_id
        permission: CAN_USE
    access_controls:
        - group_name: account users
          permission_level: CAN_USE
    '''
    app = models.resources.databricks.App.model_validate_yaml(io.StringIO(job_yaml))
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")

    name_prefix: str = Field(None, description="Prefix added to the app name")
    name_suffix: str = Field(None, description="Suffix added to the app name")

    # provider_config: AppProviderConfig = Field(None, description="")

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.name = self.name_prefix + self.name
                self.name_prefix = ""
            if self.name_suffix:
                self.name = self.name + self.name_suffix
                self.name_suffix = ""
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    app_name=f"${{resources.{self.resource_name}.name}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:App"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "access_controls",
            "name_prefix",
            "name_suffix",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
