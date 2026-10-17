from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import TagPolicy

tag_policy = TagPolicy(
    tag_key="laktory_data_classification",
    description="Sensitivity level of the data",
    values=[{"name": "public"}, {"name": "pii"}],
)


def test_tagpolicy():
    assert tag_policy.values[1].name == "pii"
    assert tag_policy.resource_key == "laktory_data_classification"
    assert tag_policy.resource_name == "tag-policy-laktory_data_classification"
    assert tag_policy.terraform_resource_type == "databricks_tag_policy"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(tag_policy)
