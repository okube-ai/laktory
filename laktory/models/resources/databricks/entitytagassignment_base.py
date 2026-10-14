# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_entity_tag_assignment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class EntityTagAssignmentProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class EntityTagAssignmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_entity_tag_assignment`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    entity_name: str = Field(
        ...,
        description="The fully qualified name of the entity to which the tag is assigned",
    )
    entity_type: str = Field(
        ..., description="The type of the entity to which the tag is assigned"
    )
    tag_key: str = Field(..., description="The key of the tag")
    tag_value: str | None = Field(None, description="The value of the tag")
    provider_config: EntityTagAssignmentProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_entity_tag_assignment"


__all__ = ["EntityTagAssignmentBase", "EntityTagAssignmentProviderConfig"]
