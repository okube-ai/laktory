from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Budget

budget = Budget(
    display_name="databricks-workspace-budget",
    alert_configurations=[
        {
            "time_period": "MONTH",
            "trigger_type": "CUMULATIVE_SPENDING_EXCEEDED",
            "quantity_type": "LIST_PRICE_DOLLARS_USD",
            "quantity_threshold": "840",
            "action_configurations": [
                {
                    "action_type": "EMAIL_NOTIFICATION",
                    "target": "abc@gmail.com",
                }
            ],
        }
    ],
    filter={
        "workspace_id": {"operator": "IN", "values": [1234567890098765]},
        "tags": [
            {"key": "Team", "value": {"operator": "IN", "values": ["Data Science"]}}
        ],
    },
)


def test_budget():
    assert budget.display_name == "databricks-workspace-budget"
    assert budget.alert_configurations[0].trigger_type == "CUMULATIVE_SPENDING_EXCEEDED"
    assert budget.alert_configurations[0].action_configurations[0].target == (
        "abc@gmail.com"
    )
    assert budget.filter.workspace_id.values == [1234567890098765]


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(budget)
