from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Dashboard
from laktory.models.resources.databricks.permissions import Permissions

dashboard = Dashboard(
    display_name="databricks-costs",
    serialized_dashboard="",
    parent_path="/.laktory/dashboards",
    warehouse_id="a7d9f2kl8mp3q6rt",
    access_controls=[{"permission_level": "CAN_READ", "group_name": "account users"}],
)


def test_dashboard():
    print(dashboard)
    assert dashboard.display_name == "databricks-costs"
    assert dashboard.access_controls[0].permission_level == "CAN_READ"
    assert dashboard.access_controls[0].group_name == "account users"


def test_dashboard_additional_resources():
    assert len(dashboard.additional_core_resources) == 1
    assert isinstance(dashboard.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(dashboard)


if __name__ == "__main__":
    test_dashboard()
