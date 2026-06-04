# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_app
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AppGitRepository(BaseModel):
    provider: str = Field(
        ...,
        description="Git provider. Case insensitive. Supported values: `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`, `awsCodeCommit`",
    )
    url: str = Field(..., description="The URL of the app once it is deployed")


class AppProviderConfig(BaseModel):
    workspace_id: str | None = Field(None)


class AppResourcesApp(BaseModel):
    name: str | None = Field(
        None, description="The name of the app to grant permission on"
    )
    permission: str | None = Field(
        None,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesDatabase(BaseModel):
    database_name: str = Field(..., description="The name of database")
    instance_name: str = Field(..., description="The name of database instance")
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesExperiment(BaseModel):
    experiment_id: str = Field(
        ..., description="The ID of the MLflow experiment to grant permission on"
    )
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesGenieSpace(BaseModel):
    name: str = Field(..., description="The name of the app to grant permission on")
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )
    space_id: str = Field(
        ...,
        description="The unique ID of Genie Space. * `app` attribute - reference to another Databricks App",
    )


class AppResourcesJob(BaseModel):
    id: str = Field(
        ...,
        description="The unique identifier of the app. * `compute_status` attribute",
    )
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesPostgres(BaseModel):
    branch: str | None = Field(
        None,
        description="The resource path of the Lakebase Autoscaling branch to grant permission on (e.g. `projects/proj-abc123/branches/branch-xyz789`)",
    )
    database: str | None = Field(
        None,
        description="The resource path of a specific database within the branch to grant permission on (e.g. `projects/proj-abc123/branches/branch-xyz789/databases/db-456`). If omitted, permission applies to the branch",
    )
    permission: str | None = Field(
        None,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesSecret(BaseModel):
    key: str = Field(..., description="Key of the secret to grant permission on")
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )
    scope: str = Field(..., description="Scope of the secret to grant permission on")


class AppResourcesServingEndpoint(BaseModel):
    name: str = Field(..., description="The name of the app to grant permission on")
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesSqlWarehouse(BaseModel):
    id: str = Field(
        ...,
        description="The unique identifier of the app. * `compute_status` attribute",
    )
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )


class AppResourcesUcSecurable(BaseModel):
    permission: str = Field(
        ...,
        description="Permission to grant on the experiment. Supported permissions are: `CAN_READ`, `CAN_EDIT`, `CAN_MANAGE`",
    )
    securable_full_name: str = Field(
        ...,
        description="The full name of UC securable, i.e. `my-catalog.my-schema.my-volume`",
    )
    securable_type: str = Field(
        ...,
        description="The type of UC securable. Supported values are `CONNECTION`, `FUNCTION`, `TABLE`, `VOLUME`",
    )


class AppResources(BaseModel):
    app: AppResourcesApp | None = Field(None)
    database: AppResourcesDatabase | None = Field(
        None,
        description="The resource path of a specific database within the branch to grant permission on (e.g. `projects/proj-abc123/branches/branch-xyz789/databases/db-456`). If omitted, permission applies to the branch",
    )
    description: str | None = Field(None, description="The description of the resource")
    experiment: AppResourcesExperiment | None = Field(None)
    genie_space: AppResourcesGenieSpace | None = Field(None)
    job: AppResourcesJob | None = Field(None)
    name: str = Field(..., description="The name of the app to grant permission on")
    postgres: AppResourcesPostgres | None = Field(None)
    secret: AppResourcesSecret | None = Field(None)
    serving_endpoint: AppResourcesServingEndpoint | None = Field(None)
    sql_warehouse: AppResourcesSqlWarehouse | None = Field(None)
    uc_securable: AppResourcesUcSecurable | None = Field(None)


class AppTelemetryExportDestinationsUnityCatalog(BaseModel):
    logs_table: str = Field(
        ..., description="Full name of the Unity Catalog table for OpenTelemetry logs"
    )
    metrics_table: str = Field(
        ...,
        description="Full name of the Unity Catalog table for OpenTelemetry metrics",
    )
    traces_table: str = Field(
        ...,
        description="Full name of the Unity Catalog table for OpenTelemetry traces (spans)",
    )


class AppTelemetryExportDestinations(BaseModel):
    unity_catalog: AppTelemetryExportDestinationsUnityCatalog | None = Field(None)


class AppBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_app`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(..., description="The name of the app to grant permission on")
    budget_policy_id: str | None = Field(
        None, description="The Budget Policy ID set for this resource"
    )
    compute_max_instances: int | None = Field(None)
    compute_min_instances: int | None = Field(None)
    compute_size: str | None = Field(
        None,
        description="A string specifying compute size for the App. Possible values are `MEDIUM`, `LARGE`",
    )
    description: str | None = Field(None, description="The description of the resource")
    no_compute: bool | None = Field(None)
    space: str | None = Field(None)
    usage_policy_id: str | None = Field(
        None, description="The Usage Policy ID set for this resource"
    )
    user_api_scopes: list[str] | None = Field(
        None,
        description="A list of api scopes granted to the user access token.  See [REST API docs](https://docs.databricks.com/api/workspace/api/scopes) for full list of supported scopes",
    )
    git_repository: AppGitRepository | None = Field(
        None,
        description="Git repository configuration for app deployments (see [below](#git_repository-configuration-attribute)). When specified, deployments can reference code from this repository by providing only the git reference (branch, tag, or commit)",
    )
    provider_config: AppProviderConfig | None = Field(None)
    resources: list[AppResources] | None = Field(
        None, description="A list of resources that the app have access to"
    )
    telemetry_export_destinations: list[AppTelemetryExportDestinations] | None = Field(
        None,
        description="A list of destinations to which the app's telemetry (logs, metrics, traces) is exported (see [below](#telemetry_export_destinations-configuration-attribute))",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_app"


__all__ = [
    "AppGitRepository",
    "AppProviderConfig",
    "AppResources",
    "AppResourcesApp",
    "AppResourcesDatabase",
    "AppResourcesExperiment",
    "AppResourcesGenieSpace",
    "AppResourcesJob",
    "AppResourcesPostgres",
    "AppResourcesSecret",
    "AppResourcesServingEndpoint",
    "AppResourcesSqlWarehouse",
    "AppResourcesUcSecurable",
    "AppTelemetryExportDestinations",
    "AppTelemetryExportDestinationsUnityCatalog",
    "AppBase",
]
