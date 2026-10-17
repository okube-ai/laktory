import pytest

from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import EntityTagAssignment

tag = EntityTagAssignment(
    entity_type="schemas",
    entity_name="laktory.unit_tests",
    tag_key="environment",
    tag_value="dev",
)


def test_entitytagassignment():
    assert tag.resource_key == "schemas-laktory.unit_tests-environment"
    assert tag.resource_name == (
        "entity-tag-assignment-schemas-laktory-unit_tests-environment"
    )
    assert tag.terraform_resource_type == "databricks_entity_tag_assignment"

    with pytest.raises(ValueError):
        EntityTagAssignment(entity_type="apps", entity_name="x", tag_key="k")


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(tag)
