# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_metastore
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class MetastoreBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_metastore`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host.",
    )
    default_data_access_config_id: str | None = Field(
        None,
        description="Unique identifier of the metastore's (Default) Data Access Configuration.",
    )
    delta_sharing_organization_name: str | None = Field(
        None,
        description="The organization name of a Delta Sharing entity, to be used in Databricks-to-Databricks Delta Sharing as the official name.",
    )
    delta_sharing_recipient_token_lifetime_in_seconds: int | None = Field(
        None, description="The lifetime of delta sharing recipient token in seconds."
    )
    delta_sharing_scope: str | None = Field(
        None, description="The scope of Delta Sharing enabled for the metastore."
    )
    external_access_enabled: bool | None = Field(
        None,
        description="Whether to allow non-DBR clients to directly access entities under the metastore.",
    )
    force_destroy: bool | None = Field(None)
    name: str | None = Field(
        None, description="The user-specified name of the metastore."
    )
    owner: str | None = Field(None, description="The owner of the metastore.")
    privilege_model_version: str | None = Field(
        None,
        description="Privilege model version of the metastore, of the form `major.minor` (e.g., `1.0`).",
    )
    region: str | None = Field(
        None,
        description="Cloud region which the metastore serves (e.g., `us-west-2`, `westus`).",
    )
    storage_root: str | None = Field(
        None, description="The storage root URL for metastore"
    )
    storage_root_credential_id: str | None = Field(
        None,
        description="UUID of storage credential to access the metastore storage_root.",
    )
    storage_root_credential_name: str | None = Field(
        None,
        description="Name of the storage credential to access the metastore storage_root.",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_metastore"


__all__ = ["MetastoreBase"]
