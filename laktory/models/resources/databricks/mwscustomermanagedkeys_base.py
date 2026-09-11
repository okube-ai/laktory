# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_customer_managed_keys
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsCustomerManagedKeysAwsKeyInfo(BaseModel):
    key_alias: str | None = Field(None, description="The AWS KMS key alias")
    key_arn: str = Field(
        ..., description="The AWS KMS key's Amazon Resource Name (ARN)"
    )
    key_region: str | None = Field(
        None,
        description="(Computed) The AWS region in which KMS key is deployed to. This is not required",
    )


class MwsCustomerManagedKeysGcpKeyInfo(BaseModel):
    kms_key_id: str = Field(..., description="The GCP KMS key's resource name")


class MwsCustomerManagedKeysBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_customer_managed_keys`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str = Field(
        ...,
        description="Account Id that could be found in the top right corner of [Accounts Console](https://accounts.cloud.databricks.com/)",
    )
    use_cases: list[str] = Field(
        ...,
        description="*(since v0.3.4)* List of use cases for which this key will be used. *If you've used the resource before, please add `use_cases = ['MANAGED_SERVICES']` to keep the previous behaviour.* Possible values are:",
    )
    creation_time: int | None = Field(
        None,
        description="(Integer) Time in epoch milliseconds when the customer key was created",
    )
    customer_managed_key_id: str | None = Field(
        None, description="(String) ID of the encryption key configuration object"
    )
    aws_key_info: MwsCustomerManagedKeysAwsKeyInfo | None = Field(
        None,
        description="This field is a block and is documented below. This conflicts with `gcp_key_info`",
    )
    gcp_key_info: MwsCustomerManagedKeysGcpKeyInfo | None = Field(
        None,
        description="This field is a block and is documented below. This conflicts with `aws_key_info`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_customer_managed_keys"


__all__ = [
    "MwsCustomerManagedKeysAwsKeyInfo",
    "MwsCustomerManagedKeysBase",
    "MwsCustomerManagedKeysGcpKeyInfo",
]
