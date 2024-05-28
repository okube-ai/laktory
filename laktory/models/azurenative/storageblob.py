from typing import Union
from typing import Literal
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.mwspermissionassignment import (
    MwsPermissionAssignment,
)


class StorageBlob(BaseModel, PulumiResource):
    """
    Databricks group

    Attributes
    ----------
    access_tier:
        The access tier of the storage blob.
    account_name:
        Specifies the storage account in which to create the storage container.
    blob_name:
        The name of the storage blob. Must be unique within the storage container the blob is located.
    container_name:
        The name of the storage container in which this blob should be created.
    content_md5:
        The MD5 sum of the blob contents. Cannot be defined if blob type is Append.
    content_type:
        The content type of the storage blob. Defaults to application/octet-stream.
    metadata:
        A map of custom blob metadata.
    resource_group_name:
        The name of the resource group within the user's subscription.
    type:
        The type of the storage blob to be created. Defaults to 'Block'.
    """

    access_tier: Literal["HOT", "COOL", "ARCHIVE"] = None
    account_name: str = None
    blob_name: str = None
    container_name: str = None
    content_md5: str = None
    content_type: str = None
    metadata: dict[str, str] = None
    resource_group_name: str = None
    # source: Union[pulumi.Asset, pulumi.Archive] = None
    type: Literal["BLOCK", "APPEND"] = None

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
