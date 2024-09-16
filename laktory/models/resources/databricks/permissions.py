from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Permissions(BaseModel, PulumiResource, TerraformResource):
    access_controls: list[AccessControl]
    pipeline_id: str = None
    job_id: str = None
    cluster_id: str = None
    dashboard_id: str = None
    directory_id: str = None
    directory_path: str = None
    experiment_id: str = None
    notebook_id: str = None
    notebook_path: str = None
    object_type: str = None
    registered_model_id: str = None
    repo_id: str = None
    repo_path: str = None
    serving_endpoint_id: str = None
    sql_alert_id: str = None
    sql_dashboard_id: str = None
    sql_endpoint_id: str = None
    sql_query_id: str = None
    workspace_file_id: str = None
    workspace_file_path: str = None

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Permissions"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_permissions"
