from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import PermissionAssignment

assignment = PermissionAssignment(group_name="laktory-unit-test", permissions=["USER"])


def test_permissionassignment():
    assert assignment.resource_key == "laktory-unit-test"
    assert assignment.permissions == ["USER"]
    assert assignment.terraform_resource_type == "databricks_permission_assignment"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(assignment)
