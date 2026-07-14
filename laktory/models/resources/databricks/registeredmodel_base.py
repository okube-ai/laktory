# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_registered_model
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class RegisteredModelAliases(BaseModel):
    alias_name: str | None = Field(None)
    catalog_name: str | None = Field(
        None,
        description="The name of the catalog where the schema and the registered model reside. *Change of this parameter forces recreation of the resource.*",
    )
    id: str | None = Field(
        None,
        description="Equal to the full name of the model (`catalog_name.schema_name.name`) and used to identify the model uniquely across the metastore",
    )
    model_name: str | None = Field(None)
    schema_name: str | None = Field(
        None,
        description="The name of the schema where the registered model resides. *Change of this parameter forces recreation of the resource.*",
    )
    version_num: int | None = Field(None)


class RegisteredModelBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_registered_model`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    browse_only: bool | None = Field(None)
    catalog_name: str | None = Field(
        None,
        description="The name of the catalog where the schema and the registered model reside. *Change of this parameter forces recreation of the resource.*",
    )
    comment: str | None = Field(
        None, description="The comment attached to the registered model"
    )
    created_at: int | None = Field(None)
    created_by: str | None = Field(None)
    metastore_id: str | None = Field(None)
    name: str | None = Field(
        None,
        description="The name of the registered model.  *Change of this parameter forces recreation of the resource.*",
    )
    owner: str | None = Field(None, description="Name of the registered model owner")
    schema_name: str | None = Field(
        None,
        description="The name of the schema where the registered model resides. *Change of this parameter forces recreation of the resource.*",
    )
    storage_location: str | None = Field(
        None,
        description="The storage location under which model version data files are stored.  If the URL contains special characters, such as space, `&`, etc., they should be percent-encoded (space -> `%20`, etc.). *Change of this parameter forces recreation of the resource.*",
    )
    updated_at: int | None = Field(None)
    updated_by: str | None = Field(None)
    aliases: list[RegisteredModelAliases] | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_registered_model"


__all__ = ["RegisteredModelAliases", "RegisteredModelBase"]
