from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsWorkspaces

workspace = MwsWorkspaces(
    account_id="00000000-0000-0000-0000-000000000000",
    workspace_name="my-workspace",
    aws_region="us-east-1",
    credentials_id="cred-id",
    storage_configuration_id="storage-id",
)


def test_mwsworkspaces():
    assert workspace.account_id == "00000000-0000-0000-0000-000000000000"
    assert workspace.workspace_name == "my-workspace"
    assert workspace.aws_region == "us-east-1"
    assert workspace.credentials_id == "cred-id"
    assert workspace.storage_configuration_id == "storage-id"
    assert workspace.resource_key == "my-workspace"
    assert workspace.terraform_resource_type == "databricks_mws_workspaces"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(workspace)
