from laktory.models.resources.databricks import Dashboard

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


if __name__ == "__main__":
    test_dashboard()
