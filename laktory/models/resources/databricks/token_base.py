# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_token
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class TokenBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_token`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    comment: str | None = Field(
        None,
        description="(String) Comment that will appear on the user's settings page for this token",
    )
    creation_time: int | None = Field(None)
    expiry_time: int | None = Field(None)
    lifetime_seconds: int | None = Field(
        None,
        description="(Integer) The lifetime of the token, in seconds. If no lifetime is specified, then expire time will be set to maximum allowed by the workspace configuration or platform",
    )
    token_id: str | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_token"


__all__ = ["TokenBase"]
