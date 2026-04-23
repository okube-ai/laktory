# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_grants
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class GrantsGrant(BaseModel):
    principal: str = Field(...)
    privileges: list[str] = Field(...)


class GrantsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_grants`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

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
    grant: list[GrantsGrant] | None = PluralField(None, plural="grants")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_grants"
