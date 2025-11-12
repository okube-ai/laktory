from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ShareObject(BaseModel):
    """Object to be shared"""
    name: str = Field(None, description="Name of the object")
    data_object_type: str = Field(None, description="Type of the data object (e.g., TABLE, VIEW)")
    added_at: int = Field(None, description="Timestamp when object was added")
    added_by: str = Field(None, description="User who added the object")
    comment: str = Field(None, description="Comment about the shared object")
    history_data_sharing_status: str = Field(
        None, description="History data sharing status"
    )
    partition_values: list[str] = Field(
        None, description="Partition values for the shared object"
    )
    shared_as: str = Field(None, description="Name the object is shared as")
    start_version: int = Field(None, description="Start version for sharing")
    status: str = Field(None, description="Status of the object")


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
