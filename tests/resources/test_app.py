from laktory.models.resources.databricks import App

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
    data = app.model_dump(exclude_unset=True)
    print(data)
    assert data == {
        "access_controls": [
            {"group_name": "account users", "permission_level": "CAN_USE"}
        ],
        "description": "A dashboard app for visualizing stock prices.",
        "name": "[laktory]stocks-dash",
        "name_prefix": "",
        "resources": [
            {
                "name": "sql-warehouse",
                "sql_warehouse": {"id": "warehouse_id", "permission": "CAN_USE"},
            }
        ],
    }
