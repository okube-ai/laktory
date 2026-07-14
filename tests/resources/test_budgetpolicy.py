from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import BudgetPolicy

budget_policy = BudgetPolicy(
    policy_name="my-budget-policy",
    custom_tags=[{"key": "mykey", "value": "myvalue"}],
)


def test_budget_policy():
    assert budget_policy.policy_name == "my-budget-policy"
    assert budget_policy.custom_tags[0].key == "mykey"
    assert budget_policy.custom_tags[0].value == "myvalue"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(budget_policy)
