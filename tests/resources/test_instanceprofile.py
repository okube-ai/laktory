from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import InstanceProfile

profile = InstanceProfile(
    instance_profile_arn="arn:aws:iam::000000000000:instance-profile/my-role",
)


def test_instanceprofile():
    assert (
        profile.instance_profile_arn
        == "arn:aws:iam::000000000000:instance-profile/my-role"
    )
    assert profile.resource_key == "arn-aws-iam--000000000000-instance-profile/my-role"
    assert profile.resource_name == "arn-aws-iam-000000000000-instance-profile-my-role"
    assert profile.terraform_resource_type == "databricks_instance_profile"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(profile)
