# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_sql_global_config
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class SqlGlobalConfigBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_sql_global_config`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    data_access_config: dict[str, str] | None = Field(
        None,
        description="Data access configuration for databricks_sql_endpoint, such as configuration for an external Hive metastore, Hadoop Filesystem configuration, etc.  Please note that the list of supported configuration properties is limited, so refer to the [documentation](https://docs.databricks.com/sql/admin/data-access-configuration.html#supported-properties) for a full list.  Apply will fail if you're specifying not permitted configuration",
    )
    enable_serverless_compute: bool | None = Field(None)
    google_service_account: str | None = Field(
        None,
        description="used to access GCP services, such as Cloud Storage, from databricks_sql_endpoint. Please note that this parameter is only for GCP, and will generate an error if used on other clouds",
    )
    instance_profile_arn: str | None = Field(
        None,
        description="databricks_instance_profile used to access storage from databricks_sql_endpoint. Please note that this parameter is only for AWS, and will generate an error if used on other clouds",
    )
    security_policy: str | None = Field(
        None,
        description="The policy for controlling access to datasets. Default value: `DATA_ACCESS_CONTROL`, consult documentation for list of possible values",
    )
    sql_config_params: dict[str, str] | None = Field(
        None,
        description="SQL Configuration Parameters let you override the default behavior for all sessions with all endpoints",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_global_config"


__all__ = ["SqlGlobalConfigBase"]
