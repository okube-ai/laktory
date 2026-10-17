# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_branch
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class PostgresBranchProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresBranchSpec(BaseModel):
    expire_time: str | None = Field(
        None,
        description="Absolute expiration time for the branch. Empty if expiration is disabled",
    )
    is_protected: bool | None = Field(
        None, description="Whether the branch is protected"
    )
    no_expiry: bool | None = Field(
        None,
        description="Explicitly disable expiration. When set to true, the branch will not expire. If set to false, the request is invalid; provide either ttl or expire_time instead. Mutually exclusive with `expire_time` and `ttl`",
    )
    source_branch: str | None = Field(
        None,
        description="The name of the source branch from which this branch was created. Format: projects/{project_id}/branches/{branch_id}",
    )
    source_branch_lsn: str | None = Field(
        None,
        description="The Log Sequence Number (LSN) on the source branch from which this branch was created",
    )
    source_branch_time: str | None = Field(
        None,
        description="The point in time on the source branch from which this branch was created",
    )
    source_snapshot: str | None = Field(
        None,
        description="The snapshot this branch was restored from. Set only for branches created by restoring a snapshot; unset for all other branches. Format: projects/{project_id}/snapshots/{snapshot_id}",
    )
    ttl: str | None = Field(
        None,
        description="Relative time-to-live duration. When set, the branch will expire at creation_time + ttl. Mutually exclusive with `expire_time` and `no_expiry`",
    )


class PostgresBranchBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_branch`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    branch_id: str = Field(..., description="Part of the resource name")
    parent: str = Field(
        ...,
        description="The project containing this branch (API resource hierarchy). Format: projects/{project_id}",
    )
    purge_on_delete: bool | None = Field(
        None,
        description="If true, permanently delete the branch; if false, soft delete",
    )
    replace_existing: bool | None = Field(
        None,
        description="If true, update the branch if it already exists instead of returning an error",
    )
    provider_config: PostgresBranchProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    spec: PostgresBranchSpec | None = Field(
        None, description="The spec contains the branch configuration"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_branch"


__all__ = ["PostgresBranchBase", "PostgresBranchProviderConfig", "PostgresBranchSpec"]
