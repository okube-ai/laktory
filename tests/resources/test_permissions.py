from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Permissions
from laktory.models.resources.databricks.accesscontrol import AccessControl

access_controls = [
    AccessControl(user_name="user1", permission_level="CAN_MANAGE"),
    AccessControl(user_name="user2", permission_level="CAN_RUN"),
]
permissions = Permissions(access_controls=access_controls, pipeline_id="pipeline_123")


def test_permissions_initialization():
    assert permissions.access_control == access_controls
    assert permissions.pipeline_id == "pipeline_123"
    assert permissions.job_id is None
    assert permissions.cluster_id is None


def test_permissions_terraform_resource_type():
    permissions = Permissions(access_controls=[])
    assert permissions.terraform_resource_type == "databricks_permissions"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(permissions)
