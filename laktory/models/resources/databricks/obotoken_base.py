# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_obo_token
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class OboTokenBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_obo_token`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    application_id: str = Field(
        ...,
        description="Application ID of [databricks_service_principal](service_principal.md#application_id) to create a PAT token for",
    )
    comment: str | None = Field(
        None,
        description="(String, Optional) Comment that describes the purpose of the token",
    )
    lifetime_seconds: float | None = Field(
        None,
        description="(Integer, Optional) The number of seconds before the token expires. Token resource is re-created when it expires. If no lifetime is specified, the token remains valid indefinitely",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_obo_token"


__all__ = ["OboTokenBase"]
