# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_credentials
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsCredentialsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_credentials`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    credentials_name: str = Field(..., description="name of credentials to register")
    role_arn: str = Field(..., description="ARN of cross-account role")
    account_id: str | None = Field(
        None,
        description="**(Deprecated)** Maintained for backwards compatibility and will be removed in a later version. It should now be specified under a provider instance where `host = 'https://accounts.cloud.databricks.com'`",
    )
    creation_time: int | None = Field(
        None, description="(Integer) time of credentials registration"
    )
    credentials_id: str | None = Field(
        None, description="(String) identifier of credentials"
    )
    external_id: str | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_credentials"


__all__ = ["MwsCredentialsBase"]
