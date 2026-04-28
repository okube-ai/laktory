from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks.grant import Grant
from laktory.models.resources.databricks.grants import Grants


def test_model():
    table = Table(
        name="slv_stock_prices",
        columns=[
            {"name": "created_at", "type": "timestamp"},
            {"name": "symbol", "type": "string"},
        ],
        catalog_name="dev",
        schema_name="markets",
        table_type="MANAGED",
    )
    assert table.name == "slv_stock_prices"
    assert table.full_name == "dev.markets.slv_stock_prices"
    assert table.column[0].name == "created_at"


def test_table_additional_resources_grant():
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        table_type="MANAGED",
        grant={"principal": "users", "privileges": ["SELECT"]},
    )
    assert len(table.additional_core_resources) == 1
    assert isinstance(table.additional_core_resources[0], Grant)


def test_table_additional_resources_grants():
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        table_type="MANAGED",
        grants=[
            {"principal": "users", "privileges": ["SELECT"]},
            {"principal": "admin", "privileges": ["MODIFY"]},
        ],
    )
    assert len(table.additional_core_resources) == 1
    assert isinstance(table.additional_core_resources[0], Grants)


def test_terraform_plan():
    skip_terraform_plan()
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        table_type="MANAGED",
    )
    plan_resource(table)


if __name__ == "__main__":
    test_model()
