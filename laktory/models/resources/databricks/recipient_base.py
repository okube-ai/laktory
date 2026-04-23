# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_recipient
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class RecipientIpAccessList(BaseModel):
    allowed_ip_addresses: list[str] | None = Field(
        None, description="Allowed IP Addresses in CIDR notation. Limit of 100"
    )


class RecipientPropertiesKvpairs(BaseModel):
    properties: dict[str, str] = Field(...)


class RecipientTokens(BaseModel):
    pass


class RecipientBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_recipient`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    authentication_type: str = Field(
        ...,
        description="The delta sharing authentication type. Valid values are `TOKEN` and `DATABRICKS`",
    )
    name: str = Field(
        ..., description="Name of recipient. Change forces creation of a new resource"
    )
    comment: str | None = Field(None, description="Description about the recipient")
    data_recipient_global_metastore_id: str | None = Field(
        None, description="Required when `authentication_type` is `DATABRICKS`"
    )
    expiration_time: float | None = Field(
        None, description="Expiration timestamp of the token in epoch milliseconds"
    )
    owner: str | None = Field(
        None, description="Username/groupname/sp application_id of the recipient owner"
    )
    sharing_code: str | None = Field(
        None, description="The one-time sharing code provided by the data recipient"
    )
    ip_access_list: RecipientIpAccessList | None = Field(
        None, description="Recipient IP access list"
    )
    properties_kvpairs: RecipientPropertiesKvpairs | None = Field(
        None,
        description="Recipient properties - object consisting of following fields: * `properties` (Required) a map of string key-value pairs with recipient's properties.  Properties with name starting with `databricks.` are reserved",
    )
    tokens: list[RecipientTokens] | None = Field(
        None,
        description="List of Recipient Tokens. This field is only present when the authentication_type is TOKEN. Each list element is an object with following attributes:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_recipient"
