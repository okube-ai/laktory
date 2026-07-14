# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_ip_access_list
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class IpAccessListBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_ip_access_list`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    ip_addresses: list[str] = Field(
        ..., description="A string list of IP addresses and CIDR ranges"
    )
    label: str = Field(
        ..., description="This is the display name for the given IP ACL List"
    )
    list_type: str = Field(..., description="Can only be 'ALLOW' or 'BLOCK'")
    enabled: bool | None = Field(
        None,
        description="Boolean `true` or `false` indicating whether this list should be active.  Defaults to `true`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_ip_access_list"


__all__ = ["IpAccessListBase"]
