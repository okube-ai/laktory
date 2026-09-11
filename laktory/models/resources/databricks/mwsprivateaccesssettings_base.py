# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_private_access_settings
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsPrivateAccessSettingsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_private_access_settings`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    private_access_settings_name: str = Field(
        ..., description="Name of Private Access Settings in Databricks Account"
    )
    region: str = Field(
        ..., description="Region of AWS VPC or the Google Cloud VPC network"
    )
    account_id: str | None = Field(None)
    allowed_vpc_endpoint_ids: list[str] | None = Field(
        None,
        description="An array of databricks_mws_vpc_endpoint `vpc_endpoint_id` (not `id`). Only used when `private_access_level` is set to `ENDPOINT`. This is an allow list of databricks_mws_vpc_endpoint that in your account that can connect to your databricks_mws_workspaces over AWS PrivateLink. If hybrid access to your workspace is enabled by setting `public_access_enabled` to true, then this control only works for PrivateLink connections. To control how your workspace is accessed via public internet, see the article for databricks_ip_access_list",
    )
    private_access_level: str | None = Field(
        None,
        description="The private access level controls which VPC endpoints can connect to the UI or API of any workspace that attaches this private access settings object. `ACCOUNT` level access _(default)_ lets only databricks_mws_vpc_endpoint that are registered in your Databricks account connect to your databricks_mws_workspaces. `ENDPOINT` level access lets only specified databricks_mws_vpc_endpoint connect to your workspace. Please see the `allowed_vpc_endpoint_ids` documentation for more details",
    )
    private_access_settings_id: str | None = Field(
        None,
        description="Canonical unique identifier of Private Access Settings in Databricks Account",
    )
    public_access_enabled: bool | None = Field(
        None,
        description="If `true`, the databricks_mws_workspaces can be accessed over the databricks_mws_vpc_endpoint as well as over the public network. In such a case, you could also configure an databricks_ip_access_list for the workspace, to restrict the source networks that could be used to access it over the public network. If `false`, the workspace can be accessed only over VPC endpoints, and not over the public network. Once explicitly set, this field becomes mandatory",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_private_access_settings"


__all__ = ["MwsPrivateAccessSettingsBase"]
