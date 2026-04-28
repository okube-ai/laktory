from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Alert

alert = Alert(
    display_name="My Alert",
    parent_path="/alerts",
    query_id="1",
    condition={
        "op": "GREATER_THAN",
        "operand": {"column": {"name": "value"}},
        "threshold": {"value": {"double_value": 42.0}},
    },
)


def test_alert():
    assert alert.display_name == "My Alert"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(alert)


if __name__ == "__main__":
    test_alert()
