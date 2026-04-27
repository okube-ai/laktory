# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_ncc_binding
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsNccBindingBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_ncc_binding`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    network_connectivity_config_id: str = Field(
        ...,
        description="Canonical unique identifier of Network Connectivity Config in Databricks Account",
    )
    workspace_id: int = Field(
        ...,
        description="Identifier of the workspace to attach the NCC to. Change forces creation of a new resource",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_ncc_binding"


__all__ = ["MwsNccBindingBase"]
