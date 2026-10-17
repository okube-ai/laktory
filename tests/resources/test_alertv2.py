from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AlertV2
from laktory.models.resources.databricks.permissions import Permissions

alert = AlertV2(
    display_name="My Alert",
    name_prefix="[dev] ",
    parent_path="/alerts",
    warehouse_id="a7d9f2kl8mp3q6rt",
    query_text="SELECT 42 AS value",
    evaluation={
        "comparison_operator": "GREATER_THAN",
        "source": {"name": "value"},
        "threshold": {"value": {"double_value": 40.0}},
    },
    schedule={"quartz_cron_schedule": "0 0 8 * * ?", "timezone_id": "UTC"},
    access_controls=[{"group_name": "account users", "permission_level": "CAN_READ"}],
)


def test_alertv2():
    assert alert.display_name == "[dev] My Alert"
    assert alert.parent_path == "/.laktory/alerts"
    assert alert.resource_name == "alert-v2-dev-_My_Alert"
    assert alert.terraform_resource_type == "databricks_alert_v2"
    props = alert.terraform_properties
    assert props["parent_path"] == "/.laktory/alerts"
    assert "access_controls" not in props

    (permissions,) = alert.additional_core_resources
    assert isinstance(permissions, Permissions)
    assert permissions.alert_v2_id == f"${{resources.{alert.resource_name}.id}}"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(alert)
