# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_workspace_network_option
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceNetworkOptionBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_workspace_network_option`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    network_policy_id: str | None = Field(
        None,
        description="The network policy ID to apply to the workspace. This controls the network access rules for all serverless compute resources in the workspace. Each workspace can only be linked to one policy at a time. If no policy is explicitly assigned, the workspace will use 'default-policy'",
    )
    workspace_id: int | None = Field(None, description="The workspace ID")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_network_option"


__all__ = ["WorkspaceNetworkOptionBase"]
