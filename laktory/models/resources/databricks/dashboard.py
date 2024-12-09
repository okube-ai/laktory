from pathlib import Path
from typing import Any
from typing import Union
from pydantic import model_validator
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions


class Dashboard(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Lakeview Dashboard

    Attributes
    ----------
    access_controls:
        Dashboard access controls
    dirpath:
        Workspace directory inside rootpath in which the dashboard is deployed.
        Used only if `parent_path` is not specified.
    display_name:
        The display name of the dashboard.
    embed_credentials:
        Whether to embed credentials in the dashboard. Default is true.
    file_path:
        The path to the dashboard JSON file. Conflicts with serialized_dashboard.
    name_prefix:
        Prefix added to the dashboard display name
    name_suffix:
        Suffix added to the dashboard display name
    parent_path:
        The workspace path of the folder containing the dashboard. Includes leading slash and no trailing slash.
        If folder doesn't exist, it will be created.
    rootpath:
        Root directory to which all dashboards are deployed to. Can also be
        configured by settings LAKTORY_WORKSPACE_LAKTORY_ROOT environment
        variable. Default is `/.laktory/`. Used only if `parent_path` is not
        specified.
    serialized_dashboard:
        The contents of the dashboard in serialized string form. Conflicts with file_path.
    warehouse_id:
        The warehouse ID used to run the dashboard.

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

    access_controls: list[AccessControl] = []
    dirpath: str = None
    display_name: str
    embed_credentials: bool = None
    file_path: str = None
    name_prefix: str = None
    name_suffix: str = None
    parent_path: str = None
    path: str = None
    rootpath: str = None
    serialized_dashboard: str = None
    warehouse_id: str

    @model_validator(mode="after")
    def set_paths(self) -> Any:

        # Parent Path explicitly set
        if self.parent_path:
            return self

        # root
        if self.rootpath is None:
            self.rootpath = settings.workspace_laktory_root

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        # parent_path
        _path = Path(self.rootpath) / self.dirpath
        self.parent_path = _path.as_posix()

        return self

    @model_validator(mode="after")
    def update_name(self) -> Any:
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
                    dashboard_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Dashboard"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "access_controls",
            "dirpath",
            "rootpath",
            "name_prefix",
            "name_suffix",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_dashboard"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
