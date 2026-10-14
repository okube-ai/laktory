# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_system_schema
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SystemSchemaBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_system_schema`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    schema_: str = Field(
        ...,
        description="name of the system schema",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_system_schema"


__all__ = ["SystemSchemaBase"]
