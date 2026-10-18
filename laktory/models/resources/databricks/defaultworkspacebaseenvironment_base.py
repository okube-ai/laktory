# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_environments_default_workspace_base_environment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class DefaultWorkspaceBaseEnvironmentProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class DefaultWorkspaceBaseEnvironmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_environments_default_workspace_base_environment`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    cpu_workspace_base_environment: str | None = Field(
        None,
        description="The default workspace base environment for CPU compute. Format: workspace-base-environments/{workspace_base_environment}",
    )
    gpu_workspace_base_environment: str | None = Field(
        None,
        description="The default workspace base environment for GPU compute. Format: workspace-base-environments/{workspace_base_environment}",
    )
    provider_config: DefaultWorkspaceBaseEnvironmentProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_environments_default_workspace_base_environment"


__all__ = [
    "DefaultWorkspaceBaseEnvironmentBase",
    "DefaultWorkspaceBaseEnvironmentProviderConfig",
]
