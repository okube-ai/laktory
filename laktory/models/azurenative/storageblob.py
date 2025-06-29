from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class StorageBlob(BaseModel, PulumiResource):
    """
    Databricks group
    """

    access_tier: Literal["HOT", "COOL", "ARCHIVE"] = Field(
        None, description="The access tier of the storage blob."
    )
    account_name: str = Field(
        None,
        description="Specifies the storage account in which to create the storage container.",
    )
    blob_name: str = Field(
        None,
        description="The name of the storage blob. Must be unique within the storage container the blob is located.",
    )
    container_name: str = Field(
        None,
        description="The name of the storage container in which this blob should be created.",
    )
    content_md5: str = Field(
        None,
        description="The MD5 sum of the blob contents. Cannot be defined if blob type is Append.",
    )
    content_type: str = Field(
        None,
        description="The content type of the storage blob. Defaults to application/octet-stream.",
    )
    metadata: dict[str, str] = Field(None, description="A map of custom blob metadata.")
    resource_group_name: str = Field(
        None,
        description="The name of the resource group within the user's subscription.",
    )
    # source: Union[pulumi.Asset, pulumi.Archive] = None
    type: Literal["BLOCK", "APPEND"] = Field(
        None,
        description="The type of the storage blob to be created. Defaults to 'Block'.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Blob name"""
        return self.blob_name

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "azure-native:storage:Blob"

    # # ----------------------------------------------------------------------- #
    # # Terraform Properties                                                    #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def terraform_resource_type(self) -> str:
    #     return "databricks_group"
    #
    # @property
    # def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
    #     return self.pulumi_excludes
