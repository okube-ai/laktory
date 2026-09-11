# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_storage_configurations
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsStorageConfigurationsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_storage_configurations`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str = Field(
        ...,
        description="Account Id that could be found in the top right corner of [Accounts Console](https://accounts.cloud.databricks.com/)",
    )
    bucket_name: str = Field(..., description="name of AWS S3 bucket")
    storage_configuration_name: str = Field(
        ..., description="name under which this storage configuration is stored"
    )
    role_arn: str | None = Field(
        None,
        description="The ARN of the IAM role that Databricks will assume to access the S3 bucket. This allows sharing an S3 bucket between root storage and the default catalog for a workspace. See the [Databricks API documentation](https://docs.databricks.com/api/account/storage/create) for more details",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_storage_configurations"


__all__ = ["MwsStorageConfigurationsBase"]
