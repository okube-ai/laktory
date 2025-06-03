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


if __name__ == "__main__":
    test_alert()
