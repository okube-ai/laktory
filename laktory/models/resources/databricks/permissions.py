from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Permissions(BaseModel, PulumiResource, TerraformResource):
    access_controls: list[AccessControl] = Field(
        ..., description="Access controls list"
    )
    authorization: str = Field(None, description="")
    pipeline_id: str = Field(None, description="")
    job_id: str = Field(None, description="")
    cluster_id: str = Field(None, description="")
    cluster_policy_id: str = Field(None, description="")
    dashboard_id: str = Field(None, description="")
    directory_id: str = Field(None, description="")
    directory_path: str = Field(None, description="")
    experiment_id: str = Field(None, description="")
    notebook_id: str = Field(None, description="")
    notebook_path: str = Field(None, description="")
    object_type: str = Field(None, description="")
    registered_model_id: str = Field(None, description="")
    repo_id: str = Field(None, description="")
    repo_path: str = Field(None, description="")
    serving_endpoint_id: str = Field(None, description="")
    sql_alert_id: str = Field(None, description="")
    sql_dashboard_id: str = Field(None, description="")
    sql_endpoint_id: str = Field(None, description="")
    sql_query_id: str = Field(None, description="")
    workspace_file_id: str = Field(None, description="")
    workspace_file_path: str = Field(None, description="")

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
