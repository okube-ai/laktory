from laktory.models.resources.databricks import AccessControlRuleSet

acrs = AccessControlRuleSet(
    name="test",
    grant_rules=[{"role": "some_role", "principals": ["p0", "p1"]}],
)


def test_accesscontrolruleset():
    assert acrs.name == "test"
