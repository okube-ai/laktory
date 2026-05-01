# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_workspace_binding
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class WorkspaceBindingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_workspace_binding`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    workspace_id: int = Field(
        ...,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )
    binding_type: str | None = Field(
        None,
        description="Binding mode. Default to `BINDING_TYPE_READ_WRITE`. Possible values are `BINDING_TYPE_READ_ONLY`, `BINDING_TYPE_READ_WRITE`",
    )
    catalog_name: str | None = Field(None)
    securable_name: str | None = Field(
        None, description="Name of securable. Change forces creation of a new resource"
    )
    securable_type: str | None = Field(
        None,
        description="Type of securable. Can be `catalog`, `external_location`, `storage_credential` or `credential`. Default to `catalog`. Change forces creation of a new resource",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_workspace_binding"


__all__ = ["WorkspaceBindingBase"]
