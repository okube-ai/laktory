from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.grants.externallocationgrant import ExternalLocationGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ExternalLocationEncryptionDetailsSseEncryptionDetails(BaseModel):
    algorith: str = Field(None, description="")
    aws_kms_key_arn: str = Field(None, description="")


class ExternalLocationEncryptionDetails(BaseModel):
    sse_encryption_details: ExternalLocationEncryptionDetailsSseEncryptionDetails = (
        Field(None, description="")
    )


class ExternalLocation(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks External Location

    Examples
    --------
    ```py
    ```
    """

    access_point: str = Field(
        None,
        description="The ARN of the s3 access point to use with the external location (AWS).",
    )
    comment: str = Field(None, description="User-supplied free-form text.")
    credential_name: str = Field(
        None,
        description="Name of the databricks.StorageCredential to use with this external location.",
    )
    encryption_details: ExternalLocationEncryptionDetails = Field(
        None,
        description="""
    The options for Server-Side Encryption to be used by each Databricks s3 client when connecting to S3 cloud 
    storage (AWS).
    """,
    )
    force_destroy: bool = Field(
        None, description="Destroy external location regardless of its dependents."
    )
    force_update: bool = Field(
        None, description="Update external location regardless of its dependents."
    )
    grant: Union[ExternalLocationGrant, list[ExternalLocationGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the External Location and authoritative for a specific principal.
    Other principals within the grants are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[ExternalLocationGrant] = Field(
        None,
        description="""
    Grants operating on the External Location and authoritative for all principals. Replaces any existing grants 
    defined inside or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    metastore_id: str = Field(None, description="Metastore ID")
    name: str = Field(
        None,
        description="""
    Name of External Location, which must be unique within the databricks_metastore. Change forces creation of a new
    resource.
    """,
    )
    owner: str = Field(
        None,
        description="Username/groupname/sp application_id of the external location owner.",
    )
    read_only: bool = Field(
        None, description="Indicates whether the external location is read-only."
    )
    skip_validation: bool = Field(
        None,
        description="Suppress validation errors if any & force save the external location",
    )
    url: str = Field(
        None,
        description="""
    Path URL in cloud storage, of the form: s3://[bucket-host]/[bucket-dir] (AWS), abfss://[user]@[host]/[path]
    (Azure), gs://[bucket-host]/[bucket-dir] (GCP).
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - external location grants
        """
        resources = []

        # External Location Grants
        resources += self.get_grants_additional_resources(
            object={"external_location": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ExternalLocation"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_external_location"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
