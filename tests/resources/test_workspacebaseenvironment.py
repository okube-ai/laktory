from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import DefaultWorkspaceBaseEnvironment
from laktory.models.resources.databricks import WorkspaceBaseEnvironment

environment = WorkspaceBaseEnvironment(
    display_name="laktory",
    filepath="/Volumes/laktory/unit_tests/environments/laktory.yaml",
)
default = DefaultWorkspaceBaseEnvironment(
    cpu_workspace_base_environment="workspace-base-environments/laktory-12345"
)


def test_workspacebaseenvironment():
    assert environment.resource_name == "workspace-base-environment-laktory"
    assert (
        environment.terraform_resource_type
        == "databricks_environments_workspace_base_environment"
    )
    assert default.resource_name == "default-workspace-base-environment"
    assert (
        default.terraform_resource_type
        == "databricks_environments_default_workspace_base_environment"
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(environment)
    plan_resource(default)
