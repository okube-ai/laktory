# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_secret
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
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
    string_value: str | None = Field(
        None,
        description="(String) Specifies text data that you want to encrypt and store in the secret. This is required if `string_value_wo` is not set. * `string_value_wo` (Optional) (String) Specifies text data that you want to encrypt and store in the secret. This is required if `string_value` is not set. * `string_value_wo_version` (Optional) (Integer) Use together with string_value_wo to trigger an update. Increment this value when an update to `string_value_wo` is required",
    )
    string_value_wo: str | None = Field(None)
    string_value_wo_version: int | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret"


__all__ = ["SecretBase"]
