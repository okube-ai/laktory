# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_project
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresProjectInitialBranchSpec(BaseModel):
    is_protected: bool | None = Field(
        None,
        description="Whether the initial default branch should be protected from deletion",
    )


class PostgresProjectInitialEndpointSpecGroup(BaseModel):
    enable_readable_secondaries: bool | None = Field(
        None,
        description="Whether to allow read-only connections to read-write endpoints. Only relevant for read-write endpoints where size.max > 1",
    )
    max: int = Field(
        ...,
        description="The maximum number of computes in the endpoint group. Currently, this must be equal to min. Set to 1 for single compute endpoints, to disable HA. To manually suspend all computes in an endpoint group, set disabled to true on the EndpointSpec",
    )
    min: int = Field(
        ...,
        description="The minimum number of computes in the endpoint group. Currently, this must be equal to max. This must be greater than or equal to 1",
    )


class PostgresProjectInitialEndpointSpec(BaseModel):
    autoscaling_limit_max_cu: float | None = Field(
        None, description="The maximum number of Compute Units. Minimum value is 0.5"
    )
    autoscaling_limit_min_cu: float | None = Field(
        None, description="The minimum number of Compute Units. Minimum value is 0.5"
    )
    group: PostgresProjectInitialEndpointSpecGroup | None = Field(
        None, description="Settings for HA configuration of the endpoint"
    )
    no_suspension: bool | None = Field(
        None,
        description="When set to true, explicitly disables automatic suspension (never suspend). Should be set to true when provided. Mutually exclusive with `suspend_timeout_duration`",
    )
    suspend_timeout_duration: str | None = Field(
        None,
        description="Duration of inactivity after which the compute endpoint is automatically suspended. If specified should be between 60s and 604800s (1 minute to 1 week). Mutually exclusive with `no_suspension`",
    )


class PostgresProjectProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresProjectSpecCustomTags(BaseModel):
    key: str | None = Field(None, description="The key of the custom tag")
    value: str | None = Field(None, description="The value of the custom tag")


class PostgresProjectSpecDefaultEndpointSettings(BaseModel):
    autoscaling_limit_max_cu: float | None = Field(
        None, description="The maximum number of Compute Units. Minimum value is 0.5"
    )
    autoscaling_limit_min_cu: float | None = Field(
        None, description="The minimum number of Compute Units. Minimum value is 0.5"
    )
    no_suspension: bool | None = Field(
        None,
        description="When set to true, explicitly disables automatic suspension (never suspend). Should be set to true when provided. Mutually exclusive with `suspend_timeout_duration`",
    )
    pg_settings: dict[str, str] | None = Field(
        None, description="A raw representation of Postgres settings"
    )
    suspend_timeout_duration: str | None = Field(
        None,
        description="Duration of inactivity after which the compute endpoint is automatically suspended. If specified should be between 60s and 604800s (1 minute to 1 week). Mutually exclusive with `no_suspension`",
    )


class PostgresProjectSpec(BaseModel):
    budget_policy_id: str | None = Field(
        None, description="The budget policy that is applied to the project"
    )
    custom_tags: list[PostgresProjectSpecCustomTags] | None = Field(
        None, description="The effective custom tags associated with the project"
    )
    default_branch: str | None = Field(
        None, description="The full resource path of the default branch of the project"
    )
    default_endpoint_settings: PostgresProjectSpecDefaultEndpointSettings | None = (
        Field(None, description="The effective default endpoint settings")
    )
    display_name: str | None = Field(
        None, description="The effective human-readable project name"
    )
    enable_pg_native_login: bool | None = Field(
        None,
        description="Whether to enable PG native password login on all endpoints in this project",
    )
    history_retention_duration: str | None = Field(
        None,
        description="The effective number of seconds to retain the shared history for point in time recovery",
    )
    pg_version: int | None = Field(
        None, description="The effective major Postgres version number"
    )


class PostgresProjectBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_project`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    project_id: str = Field(..., description="Part of the resource name")
    purge_on_delete: bool | None = Field(
        None,
        description="If true, permanently deletes the project (hard delete). If false or unset, performs a soft delete",
    )
    initial_branch_spec: PostgresProjectInitialBranchSpec | None = Field(
        None,
        description="Configuration for the initial default branch created as part of project creation. Allows overriding branch protection. These settings only apply at creation time and do not affect resources created after project creation",
    )
    initial_endpoint_spec: PostgresProjectInitialEndpointSpec | None = Field(
        None,
        description="Configuration settings for the initial Read/Write endpoint created inside the initial branch for a newly created project. If omitted, the initial endpoint created will have default settings, without high availability configured. This field does not apply to any endpoints created after project creation. Use spec.default_endpoint_settings to configure default settings for endpoints created after project creation",
    )
    provider_config: PostgresProjectProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresProjectSpec | None = Field(
        None,
        description="The spec contains the project configuration, including display_name, pg_version (Postgres version), history_retention_duration, and default_endpoint_settings",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_project"


__all__ = [
    "PostgresProjectBase",
    "PostgresProjectInitialBranchSpec",
    "PostgresProjectInitialEndpointSpec",
    "PostgresProjectInitialEndpointSpecGroup",
    "PostgresProjectProviderConfig",
    "PostgresProjectSpec",
    "PostgresProjectSpecCustomTags",
    "PostgresProjectSpecDefaultEndpointSettings",
]
