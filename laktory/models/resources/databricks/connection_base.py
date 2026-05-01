# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_connection
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ConnectionBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_connection`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    comment: str | None = Field(None)
    connection_type: str | None = Field(None)
    name: str | None = Field(None)
    options: dict[str, str] | None = Field(None)
    owner: str | None = Field(None)
    properties: dict[str, str] | None = Field(None)
    read_only: bool | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_connection"


__all__ = ["ConnectionBase"]
