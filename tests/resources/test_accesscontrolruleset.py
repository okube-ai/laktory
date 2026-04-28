from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AccessControlRuleSet

acrs = AccessControlRuleSet(
    name="test",
    grant_rules=[{"role": "some_role", "principals": ["p0", "p1"]}],
)


def test_accesscontrolruleset():
    assert acrs.name == "test"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(acrs)
