# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_share
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class ShareObjectPartitionValue(BaseModel):
    name: str = Field(..., description="The name of the partition column")
    op: str = Field(
        ..., description="The operator to apply for the value, one of: `EQUAL`, `LIKE`"
    )
    recipient_property_key: str | None = Field(
        None,
        description="The key of a Delta Sharing recipient's property. For example `databricks-account-id`. When this field is set, field `value` can not be set",
    )
    value: str | None = Field(
        None,
        description="The value of the partition column. When this value is not set, it means null value. When this field is set, field `recipient_property_key` can not be set",
    )


class ShareObjectPartition(BaseModel):
    value: list[ShareObjectPartitionValue] | None = PluralField(
        None,
        plural="values",
        description="The value of the partition column. When this value is not set, it means null value. When this field is set, field `recipient_property_key` can not be set",
    )


class ShareObject(BaseModel):
    cdf_enabled: bool | None = Field(
        None,
        description="Whether to enable Change Data Feed (cdf) on the shared object. When this field is set, field `history_data_sharing_status` can not be set",
    )
    comment: str | None = Field(None, description="Description about the object")
    content: str | None = Field(
        None,
        description="The content of the notebook file when the data object type is NOTEBOOK_FILE. This should be base64 encoded. Required for adding a NOTEBOOK_FILE, optional for updating, ignored for other types",
    )
    data_object_type: str = Field(
        ...,
        description="Type of the data object. Supported types: `TABLE`, `FOREIGN_TABLE`, `SCHEMA`, `VIEW`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`, `MODEL`, `NOTEBOOK_FILE`, `FUNCTION`, `FEATURE_SPEC`, and `VOLUME`",
    )
    history_data_sharing_status: str | None = Field(
        None,
        description="Whether to enable history sharing, one of: `ENABLED`, `DISABLED`. When a table has history sharing enabled, recipients can query table data by version, starting from the current table version. If not specified, clients can only query starting from the version of the object at the time it was added to the share. *NOTE*: The start_version should be less than or equal the current version of the object. When this field is set, field `cdf_enabled` can not be set",
    )
    name: str = Field(..., description="The name of the partition column")
    shared_as: str | None = Field(
        None,
        description="A user-provided alias name for **table-like data objects** within the share. Use this field for: `TABLE`, `VIEW`, `MATERIALIZED_VIEW`, `STREAMING_TABLE`, `FOREIGN_TABLE`. **Do not use this field for volumes, models, notebooks, or functions** (use `string_shared_as` instead). If not provided, the object's original name will be used. Must be a 2-part name `<schema>.<table>` containing only alphanumeric characters and underscores. The `shared_as` name must be unique within a share. Change forces creation of a new resource",
    )
    start_version: float | None = Field(
        None,
        description="The start version associated with the object for cdf. This allows data providers to control the lowest object version that is accessible by clients",
    )
    string_shared_as: str | None = Field(
        None,
        description="A user-provided alias name for **non-table data objects** within the share. Use this field for: `VOLUME`, `MODEL`, `NOTEBOOK_FILE`, `FUNCTION`. **Do not use this field for tables, views, or streaming tables** (use `shared_as` instead). Format varies by type: For volumes, models, and functions use `<schema>.<name>` (2-part name); for notebooks use the file name. Names must contain only alphanumeric characters and underscores. The `string_shared_as` name must be unique for objects of the same type within a share. Change forces creation of a new resource",
    )
    partition: list[ShareObjectPartition] | None = PluralField(
        None, plural="partitions", description="Array of partitions for the shared data"
    )


class ShareBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_share`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(..., description="The name of the partition column")
    comment: str | None = Field(None, description="Description about the object")
    owner: str | None = Field(
        None, description="User name/group name/sp application_id of the share owner"
    )
    storage_root: str | None = Field(None)
    object: list[ShareObject] | None = PluralField(None, plural="objects")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_share"
