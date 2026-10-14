from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import GroupRole

group_role = GroupRole(group_id="1234", role="account_admin")


def test_grouprole():
    assert group_role.resource_key == "account_admin-1234"
    assert group_role.terraform_resource_type == "databricks_group_role"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(group_role)
