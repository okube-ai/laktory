# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_secret
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class SecretBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_secret`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    key: str = Field(
        ...,
        description="(String) key within secret scope. Must consist of alphanumeric characters, dashes, underscores, and periods, and may not exceed 128 characters",
    )
    scope: str = Field(
        ...,
        description="(String) name of databricks secret scope. Must consist of alphanumeric characters, dashes, underscores, and periods, and may not exceed 128 characters",
    )
    string_value: str = Field(..., description="(String) super secret sensitive value")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret"


__all__ = ["SecretBase"]
