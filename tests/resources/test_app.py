from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import App
from laktory.models.resources.databricks.permissions import Permissions

app = App(
    name="stocks-dash",
    description="A dashboard app for visualizing stock prices.",
    name_prefix="[laktory]",
    resources=[
        {
            "name": "sql-warehouse",
            "sql_warehouse": {
                "id": "warehouse_id",
                "permission": "CAN_USE",
            },
        }
    ],
    access_controls=[{"permission_level": "CAN_USE", "group_name": "account users"}],
)


def test_app():
    assert app.name == "[laktory]stocks-dash"
    assert app.description == "A dashboard app for visualizing stock prices."


def test_app_additional_resources():
    assert len(app.additional_core_resources) == 1
    assert isinstance(app.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(app)
