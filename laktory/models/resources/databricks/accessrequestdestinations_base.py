# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_rfa_access_request_destinations
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AccessRequestDestinationsDestinations(BaseModel):
    destination_id: str | None = Field(
        None,
        description="The identifier for the destination. This is the email address for EMAIL destinations, the URL for URL destinations, or the unique Databricks notification destination ID for all other external destinations",
    )
    destination_type: str | None = Field(
        None,
        description="The type of the destination. Possible values are: `EMAIL`, `GENERIC_WEBHOOK`, `MICROSOFT_TEAMS`, `SLACK`, `URL`",
    )
    special_destination: str | None = Field(
        None,
        description="This field is used to denote whether the destination is the email of the owner of the securable object. The special destination cannot be assigned to a securable and only represents the default destination of the securable. The securable types that support default special destinations are: 'catalog', 'external_location', 'connection', 'credential', and 'metastore'. The **destination_type** of a **special_destination** is always EMAIL. Possible values are: `SPECIAL_DESTINATION_CATALOG_OWNER`, `SPECIAL_DESTINATION_CONNECTION_OWNER`, `SPECIAL_DESTINATION_CREDENTIAL_OWNER`, `SPECIAL_DESTINATION_EXTERNAL_LOCATION_OWNER`, `SPECIAL_DESTINATION_FUNCTION_OWNER`, `SPECIAL_DESTINATION_METASTORE_OWNER`, `SPECIAL_DESTINATION_REGISTERED_MODEL_OWNER`, `SPECIAL_DESTINATION_SCHEMA_OWNER`, `SPECIAL_DESTINATION_TABLE_OWNER`, `SPECIAL_DESTINATION_VOLUME_OWNER`",
    )


class AccessRequestDestinationsProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class AccessRequestDestinationsSecurable(BaseModel):
    full_name: str | None = Field(
        None,
        description="The full name of the securable. Redundant with the name in the securable object, but necessary for Terraform integration",
    )
    provider_share: str | None = Field(
        None,
        description="Optional. The name of the Share object that contains the securable when the securable is getting shared in D2D Delta Sharing",
    )
    type: str | None = Field(
        None,
        description="Required. The type of securable (catalog/schema/table). Optional if resource_name is present. Possible values are: `AGENT_SERVICE`, `CATALOG`, `CLEAN_ROOM`, `CONNECTION`, `CREDENTIAL`, `EXTERNAL_LOCATION`, `EXTERNAL_METADATA`, `FUNCTION`, `MCP_SERVICE`, `METASTORE`, `MODEL`, `MODEL_PROVIDER_SERVICE`, `MODEL_SERVICE`, `PIPELINE`, `PROVIDER`, `RECIPIENT`, `SCHEMA`, `SHARE`, `SKILL`, `STAGING_TABLE`, `STORAGE_CREDENTIAL`, `TABLE`, `VOLUME`",
    )


class AccessRequestDestinationsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_rfa_access_request_destinations`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    destinations: list[AccessRequestDestinationsDestinations] | None = Field(
        None, description="The access request destinations for the securable"
    )
    provider_config: AccessRequestDestinationsProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    securable: AccessRequestDestinationsSecurable | None = Field(
        None,
        description="The securable for which the access request destinations are being modified or read",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_rfa_access_request_destinations"


__all__ = [
    "AccessRequestDestinationsBase",
    "AccessRequestDestinationsDestinations",
    "AccessRequestDestinationsProviderConfig",
    "AccessRequestDestinationsSecurable",
]
