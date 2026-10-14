# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_tag_policy
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class TagPolicyProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class TagPolicyValues(BaseModel):
    name: str = Field(...)


class TagPolicyBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_tag_policy`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    tag_key: str = Field(...)
    description: str | None = Field(None)
    provider_config: TagPolicyProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    values: list[TagPolicyValues] | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_tag_policy"


__all__ = ["TagPolicyBase", "TagPolicyProviderConfig", "TagPolicyValues"]
