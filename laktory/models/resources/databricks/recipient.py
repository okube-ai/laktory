from typing import Literal
from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class RecipientPropertyKvPairs(BaseModel):
    """Additional properties for recipient"""
    pass


class Recipient(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Recipient for Delta Sharing

    A recipient is an entity that can receive shared data from a Databricks workspace.
    Recipients can be configured with authentication details to enable Delta Sharing.

    Examples
    --------
    ```py
    ```
    """

    name: str = Field(..., description="The name of the recipient")
    comment: str = Field(None, description="Comment about the recipient")
    owner: str = Field(None, description="The owner of the recipient")
    
    authentication_type: Literal["TOKEN", "DATABRICKS"] = Field(
        None, description="The authentication type for the recipient"
    )
    
    ip_access_list: list[str] = Field(
        None, description="IP access list for the recipient"
    )
    
    properties_kvpairs: dict = Field(
        None, description="Additional properties as key-value pairs"
    )

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Recipient"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_recipient"
