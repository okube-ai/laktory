import pytest

from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import WorkspaceEntityTagAssignment

tag = WorkspaceEntityTagAssignment(
    entity_type="apps",
    entity_id="laktory-app",
    tag_key="cost_center",
    tag_value="finance",
)


def test_workspaceentitytagassignment():
    assert tag.resource_key == "apps-laktory-app-cost_center"
    assert tag.resource_name == (
        "workspace-entity-tag-assignment-apps-laktory-app-cost_center"
    )
    assert tag.terraform_resource_type == "databricks_workspace_entity_tag_assignment"

    with pytest.raises(ValueError):
        WorkspaceEntityTagAssignment(entity_type="tables", entity_id="x", tag_key="k")


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(tag)
