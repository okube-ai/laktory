# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_endpoint
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresEndpointProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresEndpointSpecGroup(BaseModel):
    enable_readable_secondaries: bool | None = Field(
        None,
        description="Whether read-only connections to read-write endpoints are allowed. Only relevant if read replicas are configured by specifying size.max > 1",
    )
    max: int = Field(
        ...,
        description="The maximum number of computes in the endpoint group. Currently, this must be equal to min. Set to 1 for single compute endpoints, to disable HA. To manually suspend all computes in an endpoint group, set disabled to true on the EndpointSpec",
    )
    min: int = Field(
        ...,
        description="The minimum number of computes in the endpoint group. Currently, this must be equal to max. This must be greater than or equal to 1",
    )


class PostgresEndpointSpecSettings(BaseModel):
    pg_settings: dict[str, str] | None = Field(
        None, description="A raw representation of Postgres settings"
    )


class PostgresEndpointSpec(BaseModel):
    autoscaling_limit_max_cu: float | None = Field(
        None,
        description="The maximum number of Compute Units. The maximum value is 64. The difference between the minimum and maximum Compute Units (max - min) must not exceed 16",
    )
    autoscaling_limit_min_cu: float | None = Field(
        None, description="The minimum number of Compute Units"
    )
    disabled: bool | None = Field(
        None,
        description="Whether to restrict connections to the compute endpoint. Enabling this option schedules a suspend compute operation. A disabled compute endpoint cannot be enabled by a connection or console action",
    )
    endpoint_type: str = Field(
        ...,
        description="The endpoint type. A branch can only have one READ_WRITE endpoint. Possible values are: `ENDPOINT_TYPE_READ_ONLY`, `ENDPOINT_TYPE_READ_WRITE`",
    )
    group: PostgresEndpointSpecGroup | None = Field(
        None, description="Details on the HA configuration of the endpoint"
    )
    no_suspension: bool | None = Field(
        None,
        description="When set to true, explicitly disables automatic suspension (never suspend). Should be set to true when provided. Mutually exclusive with `suspend_timeout_duration` * `settings` (EndpointSettings, optional)",
    )
    settings: PostgresEndpointSpecSettings | None = Field(None)
    suspend_timeout_duration: str | None = Field(
        None,
        description="Duration of inactivity after which the compute endpoint is automatically suspended",
    )


class PostgresEndpointBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_endpoint`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    endpoint_id: str = Field(..., description="Part of the resource name")
    parent: str = Field(
        ...,
        description="The branch containing this endpoint (API resource hierarchy). Format: projects/{project_id}/branches/{branch_id}",
    )
    replace_existing: bool | None = Field(
        None,
        description="If true, update the endpoint if it already exists instead of returning an error",
    )
    provider_config: PostgresEndpointProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresEndpointSpec | None = Field(
        None,
        description="The spec contains the compute endpoint configuration, including autoscaling limits, suspend timeout, and disabled state",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_endpoint"


__all__ = [
    "PostgresEndpointBase",
    "PostgresEndpointProviderConfig",
    "PostgresEndpointSpec",
    "PostgresEndpointSpecGroup",
    "PostgresEndpointSpecSettings",
]
