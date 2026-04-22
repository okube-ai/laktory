# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_metastore_assignment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MetastoreAssignmentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_metastore_assignment`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    metastore_id: str = Field(
        ..., description="Unique identifier of the parent Metastore"
    )
    workspace_id: float = Field(
        ...,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )
    default_catalog_name: str | None = Field(
        None,
        description="(Deprecated) Default catalog used for this assignment. Please use [databricks_default_namespace_setting](default_namespace_setting.md) instead",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_metastore_assignment"
