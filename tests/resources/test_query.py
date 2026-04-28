from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Query
from laktory.models.resources.databricks.permissions import Permissions

query = Query(
    display_name="google-prices",
    parent_path="/queries",
    query_text="SELECT * FROM dev.finance.slv_stock_prices",
    warehouse_id="12345",
)


def test_query():
    assert query.display_name == "google-prices"


def test_query_additional_resources():
    q = Query(
        display_name="test",
        query_text="SELECT 1",
        warehouse_id="abc",
        access_controls=[{"group_name": "users", "permission_level": "CAN_VIEW"}],
    )
    assert len(q.additional_core_resources) == 1
    assert isinstance(q.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(query)


if __name__ == "__main__":
    test_query()
