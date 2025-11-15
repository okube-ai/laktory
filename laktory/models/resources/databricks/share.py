from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class SharedObjectPartitionValue(BaseModel):
    """Shared Object Partition Value"""

    name: str = Field(..., description="The name of the partition column.")
    op: str = Field(
        ..., description="The operator to apply for the value, one of: EQUAL, LIKE"
    )
    recipient_property_key: str = Field(
        None,
        description="The key of a Delta Sharing recipient's property. For example `databricks-account-id`. When this field is set, field `value` can not be set.",
    )
    value: str = Field(
        None,
        description="The value of the partition column. When this value is not set, it means null value. When this field is set, field `recipient_property_key` can not be set.",
    )


class ShareObjectPartition(BaseModel):
    """Shared Object Partition"""

    values: list[SharedObjectPartitionValue] = Field(
        None,
        description="The value of the partition column. When this value is not set, it means null value. When this field is set, field recipient_property_key can not be set.",
    )


class ShareObject(BaseModel):
    """Object to be shared"""

    name: str = Field(..., description="Name of the object")
    data_object_type: str = Field(
        ..., description="Type of the data object (e.g., TABLE, VIEW)"
    )
    added_at: int = Field(None, description="Timestamp when object was added")
    added_by: str = Field(None, description="User who added the object")
    cdf_enabled: bool = Field(
        None,
        description="Whether to enable Change Data Feed (cdf) on the shared object. When this field is set, field `history_data_sharing_status` can not be set.",
    )
    comment: str = Field(None, description="Comment about the shared object")
    content: str = Field(None, description="")
    history_data_sharing_status: str = Field(
        None, description="History data sharing status"
    )
    partitions: list[ShareObjectPartition] = Field(
        None, description="Partition values for the shared object"
    )
    shared_as: str = Field(None, description="Name the object is shared as")
    start_version: int = Field(None, description="Start version for sharing")
    status: str = Field(None, description="Status of the object")


class ShareProviderConfig(BaseModel):
    """Provider Config"""

    workspace_id: str = Field(
        ...,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with.",
    )


class Share(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Share for Delta Sharing

    A share is a container that holds the objects to be shared with recipients.
    Shares enable data sharing between Databricks workspaces.

    Examples
    --------
    ```py
    ```
    """

    name: str = Field(..., description="The name of the share")
    comment: str = Field(None, description="Comment about the share")
    owner: str = Field(None, description="The owner of the share")
    objects: list[ShareObject] = Field(
        None, description="Objects contained in the share"
    )
    provider_config: ShareProviderConfig = Field(None, description="Provider config")

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Share"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_share"
