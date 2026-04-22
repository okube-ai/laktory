# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_volume
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class VolumeBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_volume`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog_name: str = Field(
        ..., description="The name of the catalog where the schema and the volume are"
    )
    name: str = Field(..., description="The name of the volume")
    schema_name: str = Field(
        ..., description="The name of the schema where the volume is"
    )
    volume_type: str = Field(
        ...,
        description="The type of the volume. An external volume is located in the specified external location. A managed volume is located in the default location which is specified by the parent schema, or the parent catalog, or the Metastore. [Learn more]",
    )
    comment: str | None = Field(None, description="The comment attached to the volume")
    owner: str | None = Field(
        None, description="The identifier of the user who owns the volume"
    )
    storage_location: str | None = Field(
        None, description="The storage location on the cloud"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_volume"
