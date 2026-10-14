# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_app
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AppGitRepository(BaseModel):
    auto_deploy: bool | None = Field(
        None,
        description="When `true`, the app is automatically redeployed on push events to the branch configured in `git_source`. This requires `git_source` to specify a `branch`; a `tag` or `commit` cannot be used, because automatic deployment is triggered by pushes to a branch. Automatic deployment is currently supported only for the `gitHub` and `azureDevOpsServices` providers",
    )
    caller_credential_id: int | None = Field(
        None,
        description="ID of a personal access token Git credential owned by the caller, used to grant the app's service principal access to this repository. This is only applied when the app is created and is not returned by the service; changing it on an existing app is not supported",
    )
    provider: str = Field(
        ...,
        description="Git provider. Case insensitive. Supported values: `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`, `awsCodeCommit`",
    )
    url: str = Field(..., description="The URL of the app once it is deployed")


class AppGitSourceGitRepository(BaseModel):
    auto_deploy: bool | None = Field(
        None,
        description="When `true`, the app is automatically redeployed on push events to the branch configured in `git_source`. This requires `git_source` to specify a `branch`; a `tag` or `commit` cannot be used, because automatic deployment is triggered by pushes to a branch. Automatic deployment is currently supported only for the `gitHub` and `azureDevOpsServices` providers",
    )
    caller_credential_id: int | None = Field(
        None,
        description="ID of a personal access token Git credential owned by the caller, used to grant the app's service principal access to this repository. This is only applied when the app is created and is not returned by the service; changing it on an existing app is not supported",
    )
    provider: str = Field(
        ...,
        description="Git provider. Case insensitive. Supported values: `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`, `awsCodeCommit`",
    )
    url: str = Field(..., description="The URL of the app once it is deployed")


class AppGitSource(BaseModel):
    branch: str | None = Field(
        None,
        description="Git branch to check out and deploy from. Required when `git_repository.auto_deploy` is `true`, since automatic deployment tracks pushes to a branch",
    )
    commit: str | None = Field(
        None, description="Git commit SHA to check out and deploy from"
    )
    git_repository: AppGitSourceGitRepository | None = Field(
        None,
        description="Git repository configuration for app deployments (see [below](#git_repository-configuration-attribute)). When specified, deployments can reference code from this repository by providing only the git reference (branch, tag, or commit)",
    )
    resolved_commit: str | None = Field(None)
    source_code_path: str | None = Field(
        None,
        description="The snapshotted workspace file system path of the source code loaded by the deployed app. * `pending_deployment` attribute - the pending deployment of the app. A deployment is considered pending when it is being prepared for deployment to the app compute. Schema is identical to `active_deployment`",
    )
    tag: str | None = Field(None, description="Git tag to check out and deploy from")


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
        description="Git branch to check out and deploy from. Required when `git_repository.auto_deploy` is `true`, since automatic deployment tracks pushes to a branch",
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
    forward_user_access_token: bool | None = Field(None)
    no_compute: bool | None = Field(None)
    source_code_path: str | None = Field(
        None,
        description="The snapshotted workspace file system path of the source code loaded by the deployed app. * `pending_deployment` attribute - the pending deployment of the app. A deployment is considered pending when it is being prepared for deployment to the app compute. Schema is identical to `active_deployment`",
    )
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
    git_source: AppGitSource | None = Field(
        None,
        description="The Git source to deploy from, specifying the reference to check out and an optional path to the app source code within the repository configured in `git_repository` (see [below](#git_source-configuration-attribute))",
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
    "AppBase",
    "AppGitRepository",
    "AppGitSource",
    "AppGitSourceGitRepository",
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
]
