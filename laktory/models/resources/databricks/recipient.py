from typing import Literal

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class RecipientPropertyKvPairs(BaseModel):
    """Additional properties for recipient"""

    properties: dict[str, str] = Field(
        ...,
        description="a map of string key-value pairs with recipient's properties. Properties with name starting with databricks. are reserved.",
    )


class RecipientIpAccessList(BaseModel):
    """Allowed IP Address"""

    allowed_ip_addresses: list[str] = Field(
        None, description="Allowed IP Addresses in CIDR notation. Limit of 100."
    )


class RecipientToken(BaseModel):
    """Recipient Token"""

    activation_url: str = Field(
        None,
        description="Full activation URL to retrieve the access token. It will be empty if the token is already retrieved.",
    )
    created_at: int = Field(
        None,
        description="Time at which this recipient was created, in epoch milliseconds.",
    )
    created_by: str = Field(None, description="Username of recipient creator.")
    expiration_time: int = Field(
        None, description="Expiration timestamp of the token in epoch milliseconds."
    )
    id: str = Field(None, description="Unique ID of the recipient token.")
    updated_at: int = Field(
        None,
        description="Time at which this recipient was updated, in epoch milliseconds.",
    )
    updated_by: str = Field(None, description="Username of recipient Token updater.")


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
        ..., description="The authentication type for the recipient"
    )

    data_recipient_global_metastore_id: str = Field(
        None,
        description="Global metastore id associated with the recipient. Required when `authentication_type` is DATABRICKS",
    )

    expiration_time: int = Field(
        None, description="Expiration timestamp of the token in epoch milliseconds."
    )

    ip_access_list: RecipientIpAccessList = Field(
        None, description="IP access list for the recipient"
    )

    properties_kvpairs: dict = Field(
        None, description="Additional properties as key-value pairs"
    )

    region: str = Field(
        None,
        description="Cloud region of the recipient's Unity Catalog Metastore. This field is only present when the authentication_type is `DATABRICKS`.",
    )

    sharing_code: str = Field(
        None, description="The one-time sharing code provided by the data recipient."
    )

    tokens: list[RecipientToken] = Field(
        None,
        description="List of Recipient Tokens. This field is only present when the authentication_type is TOKEN. ",
    )

    # updated_at: int = Field(None, description="Time at which this recipient was updated, in epoch milliseconds.")
    # updated_by: str = Field(None, description="Username of recipient Token updater.")

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
