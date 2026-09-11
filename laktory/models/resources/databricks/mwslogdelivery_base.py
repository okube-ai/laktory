# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_log_delivery
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class MwsLogDeliveryBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_log_delivery`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str = Field(
        ...,
        description="Account Id that could be found in the top right corner of [Accounts Console](https://accounts.cloud.databricks.com/)",
    )
    credentials_id: str = Field(
        ...,
        description="The ID for a Databricks credential configuration that represents the AWS IAM role with policy and trust relationship as described in the main billable usage documentation page",
    )
    log_type: str = Field(
        ...,
        description="The type of log delivery. `BILLABLE_USAGE` and `AUDIT_LOGS` are supported",
    )
    output_format: str = Field(
        ...,
        description="The file type of log delivery. Currently `CSV` (for `BILLABLE_USAGE`) and `JSON` (for `AUDIT_LOGS`) are supported",
    )
    storage_configuration_id: str = Field(
        ...,
        description="The ID for a Databricks storage configuration that represents the S3 bucket with bucket policy as described in the main billable usage documentation page",
    )
    config_id: str | None = Field(
        None, description="Databricks log delivery configuration ID"
    )
    config_name: str | None = Field(
        None,
        description="The optional human-readable name of the log delivery configuration. Defaults to empty",
    )
    delivery_path_prefix: str | None = Field(
        None,
        description="Defaults to empty, which means that logs are delivered to the root of the bucket. The value must be a valid S3 object key. It must not start or end with a slash character",
    )
    delivery_start_time: str | None = Field(
        None,
        description="The optional start month and year for delivery, specified in YYYY-MM format. Defaults to current year and month. Usage is not available before 2019-03",
    )
    status: str | None = Field(
        None,
        description="Status of log delivery configuration. Set to ENABLED or DISABLED. Defaults to ENABLED. This is the only field you can update",
    )
    workspace_ids_filter: list[int] | None = PluralField(
        None,
        plural="workspace_ids_filters",
        description="By default, this log configuration applies to all workspaces associated with your account ID. If your account is on the multitenant version of the platform or on a select custom plan that allows multiple workspaces per account, you may have multiple workspaces associated with your account ID. You can optionally set the field as mentioned earlier to an array of workspace IDs. If you plan to use different log delivery configurations for several workspaces, set this explicitly rather than leaving it blank. If you leave this blank and your account ID gets additional workspaces in the future, this configuration will also apply to the new workspaces",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_log_delivery"


__all__ = ["MwsLogDeliveryBase"]
