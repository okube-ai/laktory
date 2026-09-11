# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_instance_profile
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class InstanceProfileBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_instance_profile`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    instance_profile_arn: str = Field(
        ...,
        description="`ARN` attribute of `aws_iam_instance_profile` output, the EC2 instance profile association to AWS IAM role. This ARN would be validated upon resource creation",
    )
    iam_role_arn: str | None = Field(
        None,
        description="The AWS IAM role ARN of the role associated with the instance profile. It must have the form `arn:aws:iam::<account-id>:role/<name>`. This field is required if your role name and instance profile name do not match and you want to use the instance profile with Databricks SQL Serverless",
    )
    is_meta_instance_profile: bool | None = Field(
        None,
        description="Whether the instance profile is a meta instance profile. Used only in [IAM credential passthrough](https://docs.databricks.com/security/credential-passthrough/iam-passthrough.html)",
    )
    skip_validation: bool | None = Field(
        None,
        description="**For advanced usage only.** If validation fails with an error message that does not indicate an IAM related permission issue, (e.g. 'Your requested instance type is not supported in your requested availability zone'), you can pass this flag to skip the validation and forcibly add the instance profile",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_instance_profile"


__all__ = ["InstanceProfileBase"]
