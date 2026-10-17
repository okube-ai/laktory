# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_workspace_entity_tag_assignment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceEntityTagAssignmentProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class WorkspaceEntityTagAssignmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_workspace_entity_tag_assignment`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    entity_id: str = Field(
        ...,
        description="The identifier of the entity to which the tag is assigned. For apps, the entity_id is the app name",
    )
    entity_type: str = Field(
        ...,
        description="The type of entity to which the tag is assigned. Allowed values are apps, dashboards, geniespaces, notebooks",
    )
    tag_key: str = Field(
        ...,
        description="The key of the tag. The characters , . : / - = and leading/trailing spaces are not allowed",
    )
    tag_value: str | None = Field(None, description="The value of the tag")
    provider_config: WorkspaceEntityTagAssignmentProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_entity_tag_assignment"


__all__ = [
    "WorkspaceEntityTagAssignmentBase",
    "WorkspaceEntityTagAssignmentProviderConfig",
]
