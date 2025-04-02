from laktory.models.resources.databricks import Permissions
from laktory.models.resources.databricks.accesscontrol import AccessControl

access_controls = [
    AccessControl(user_name="user1", permission_level="READ"),
    AccessControl(user_name="user2", permission_level="WRITE"),
]
permissions = Permissions(
    access_controls=access_controls,
    pipeline_id="pipeline_123"
)

def test_permissions_initialization():
    assert permissions.access_controls == access_controls
    assert permissions.pipeline_id == "pipeline_123"
    assert permissions.job_id == None
    assert permissions.cluster_id == None


def test_permissions_pulumi_resource_type():
    permissions = Permissions(access_controls=[])
    assert permissions.pulumi_resource_type == "databricks:Permissions"


def test_permissions_terraform_resource_type():
    permissions = Permissions(access_controls=[])
    assert permissions.terraform_resource_type == "databricks_permissions"


if __name__ == "__main__":
    test_permissions_initialization()
    test_permissions_pulumi_resource_type()
    test_permissions_terraform_resource_type()
    print("All tests passed!")
