# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_grant
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class GrantBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_grant`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    principal: str = Field(...)
    privileges: list[str] = Field(...)
    catalog: str | None = Field(None)
    credential: str | None = Field(None)
    external_location: str | None = Field(None)
    foreign_connection: str | None = Field(None)
    function: str | None = Field(None)
    metastore: str | None = Field(None)
    model: str | None = Field(None)
    pipeline: str | None = Field(None)
    recipient: str | None = Field(None)
    schema_: str | None = Field(
        None,
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    share: str | None = Field(None)
    storage_credential: str | None = Field(None)
    table: str | None = Field(None)
    volume: str | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_grant"
