from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ExternalLocationEncryptionDetailsSseEncryptionDetails(BaseModel):
    """
    Attributes
    ----------
    algorith:
    aws_kms_key_arn:
    """

    algorith: str = None
    aws_kms_key_arn: str = None


class ExternalLocationEncryptionDetails(BaseModel):
    """
    Attributes
    ----------
    sse_encryption_details:
    """

    sse_encryption_details: ExternalLocationEncryptionDetailsSseEncryptionDetails = None


class ExternalLocation(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks External Location

    Attributes
    ----------
    access_point:
        The ARN of the s3 access point to use with the external location (AWS).
    comment:
        User-supplied free-form text.
    credential_name:
        Name of the databricks.StorageCredential to use with this external location.
    encryption_details:
        The options for Server-Side Encryption to be used by each Databricks s3 client when connecting to S3 cloud
        storage (AWS).
    force_destroy:
        Destroy external location regardless of its dependents.
    force_update:
        Update external location regardless of its dependents.
    metastore_id:
        Metastore ID
    name:
        Name of External Location, which must be unique within the databricks_metastore. Change forces creation of a new
        resource.
    owner:
        Username/groupname/sp application_id of the external location owner.
    read_only:
        Indicates whether the external location is read-only.
    skip_validation:
        Suppress validation errors if any & force save the external location
    url:
        Path URL in cloud storage, of the form: s3://[bucket-host]/[bucket-dir] (AWS), abfss://[user]@[host]/[path]
        (Azure), gs://[bucket-host]/[bucket-dir] (GCP).

    Examples
    --------
    ```py
    ```
    """

    access_point: str = None
    comment: str = None
    credential_name: str = None
    encryption_details: ExternalLocationEncryptionDetails = None
    force_destroy: bool = None
    force_update: bool = None
    metastore_id: str = None
    name: str = None
    owner: str = None
    read_only: bool = None
    skip_validation: bool = None
    url: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ExternalLocation"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_external_location"
