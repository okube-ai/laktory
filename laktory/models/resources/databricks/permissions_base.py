# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_permissions
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class PermissionsAccessControl(BaseModel):
    group_name: str | None = Field(None)
    permission_level: str | None = Field(None)
    service_principal_name: str | None = Field(None)
    user_name: str | None = Field(None)


class PermissionsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_permissions`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    alert_v2_id: str | None = Field(None)
    app_name: str | None = Field(None)
    authorization: str | None = Field(None)
    cluster_id: str | None = Field(None)
    cluster_policy_id: str | None = Field(None)
    dashboard_id: str | None = Field(None)
    database_instance_name: str | None = Field(None)
    database_project_name: str | None = Field(None)
    directory_id: str | None = Field(None)
    directory_path: str | None = Field(None)
    experiment_id: str | None = Field(None)
    instance_pool_id: str | None = Field(None)
    job_id: str | None = Field(None)
    notebook_id: str | None = Field(None)
    notebook_path: str | None = Field(None)
    object_type: str | None = Field(None)
    pipeline_id: str | None = Field(None)
    registered_model_id: str | None = Field(None)
    repo_id: str | None = Field(None)
    repo_path: str | None = Field(None)
    serving_endpoint_id: str | None = Field(None)
    sql_alert_id: str | None = Field(None)
    sql_dashboard_id: str | None = Field(None)
    sql_endpoint_id: str | None = Field(None)
    sql_query_id: str | None = Field(None)
    vector_search_endpoint_id: str | None = Field(None)
    workspace_file_id: str | None = Field(None)
    workspace_file_path: str | None = Field(None)
    access_control: list[PermissionsAccessControl] | None = PluralField(
        None, plural="access_controls"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_permissions"


__all__ = ["PermissionsAccessControl", "PermissionsBase"]
