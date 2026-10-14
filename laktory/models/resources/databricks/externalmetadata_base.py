# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_external_metadata
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ExternalMetadataProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class ExternalMetadataBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_external_metadata`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    entity_type: str = Field(
        ..., description="Type of entity within the external system"
    )
    name: str = Field(..., description="Name of the external metadata object")
    system_type: str = Field(
        ...,
        description="Type of external system. Possible values are: `AMAZON_REDSHIFT`, `AZURE_SYNAPSE`, `CONFLUENT`, `DATABRICKS`, `GOOGLE_BIGQUERY`, `KAFKA`, `LOOKER`, `MICROSOFT_FABRIC`, `MICROSOFT_SQL_SERVER`, `MONGODB`, `MYSQL`, `ORACLE`, `OTHER`, `POSTGRESQL`, `POWER_BI`, `SALESFORCE`, `SAP`, `SERVICENOW`, `SNOWFLAKE`, `STREAM_NATIVE`, `TABLEAU`, `TERADATA`, `WORKDAY`",
    )
    columns: list[str] | None = Field(
        None, description="List of columns associated with the external metadata object"
    )
    description: str | None = Field(
        None, description="User-provided free-form text description"
    )
    owner: str | None = Field(None, description="Owner of the external metadata object")
    properties: dict[str, str] | None = Field(
        None,
        description="A map of key-value properties attached to the external metadata object",
    )
    url: str | None = Field(
        None, description="URL associated with the external metadata object"
    )
    provider_config: ExternalMetadataProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_external_metadata"


__all__ = ["ExternalMetadataBase", "ExternalMetadataProviderConfig"]
