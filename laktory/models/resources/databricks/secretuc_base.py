# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_secret_uc
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SecretUcProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class SecretUcBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_secret_uc`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog_name: str = Field(
        ...,
        description="The name of the catalog where the schema and the secret reside",
    )
    name: str = Field(
        ..., description="The name of the secret, relative to its parent schema"
    )
    schema_name: str = Field(
        ..., description="The name of the schema where the secret resides"
    )
    value: str = Field(
        ...,
        description="The secret value to store. This field is input-only and is not returned in responses — use the **effective_value** field (via GetSecret with **include_value** set to true) to read the secret value. The maximum size is 60 KiB (pre-encryption). Accepted content includes passwords, tokens, keys, and other sensitive credential data",
    )
    comment: str | None = Field(
        None, description="User-provided free-form text description of the secret"
    )
    expire_time: str | None = Field(
        None,
        description="User-provided expiration time of the secret. This field indicates when the secret should no longer be used and may be displayed as a warning in the UI. It is purely informational and does not trigger any automatic actions or affect the secret's lifecycle",
    )
    owner: str | None = Field(
        None,
        description="The owner of the secret. Defaults to the creating principal on creation. Can be updated to transfer ownership of the secret to another principal",
    )
    provider_config: SecretUcProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret_uc"


__all__ = ["SecretUcBase", "SecretUcProviderConfig"]
