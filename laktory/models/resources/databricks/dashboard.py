from typing import Union
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
    embed_credentials:
        Whether to embed credentials in the dashboard. Default is true.
    display_name:
        The display name of the dashboard.
    file_path:
        The path to the dashboard JSON file. Conflicts with serialized_dashboard.
    parent_path:
        The workspace path of the folder containing the dashboard. Includes leading slash and no trailing slash.
        If folder doesn't exist, it will be created.
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
    # create_time: str = None
    # dashboard_change_detected: bool = None
    # dashboard_id: str = None
    display_name: str
    embed_credentials: bool = None
    # etag: str = None
    file_path: str = None
    # lifecycle_state: str = None
    # md5: str = None
    parent_path: str = None
    path: str = None
    serialized_dashboard: str = None
    # update_time: str = None
    warehouse_id: str

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
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_dashboard"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
