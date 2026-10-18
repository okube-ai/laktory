# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_environments_workspace_base_environment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceBaseEnvironmentProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class WorkspaceBaseEnvironmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_environments_workspace_base_environment`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(
        ...,
        description="Human-readable display name for the workspace base environment",
    )
    base_environment_type: str | None = Field(
        None,
        description="The type of base environment (CPU or GPU). Possible values are: `CPU`, `GPU`",
    )
    filepath: str | None = Field(
        None, description="The WSFS or UC Volumes path to the environment YAML file"
    )
    workspace_base_environment_id: str | None = Field(
        None,
        description="The ID to use for the workspace base environment, which will become the final component of the resource name. This value should be 4-63 characters, and valid characters are `/[a-z][0-9]-/`",
    )
    provider_config: WorkspaceBaseEnvironmentProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_environments_workspace_base_environment"


__all__ = ["WorkspaceBaseEnvironmentBase", "WorkspaceBaseEnvironmentProviderConfig"]
