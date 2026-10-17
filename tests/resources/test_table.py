from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import EntityTagAssignment
from laktory.models.resources.databricks import PolicyInfo
from laktory.models.resources.databricks import Table


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


def test_table_tags_and_policies():
    table = Table(
        name="slv_trades",
        catalog_name="dev",
        schema_name="markets",
        table_type="MANAGED",
        columns=[
            {"name": "symbol", "type": "string"},
            {"name": "trader_email", "type": "string", "tags": {"pii": "email"}},
        ],
        tags={"data_classification": "confidential"},
        policies=[
            {
                "name": "filter_region",
                "policy_type": "POLICY_TYPE_ROW_FILTER",
                "for_securable_type": "TABLE",
                "to_principals": ["account users"],
                "row_filter": {"function_name": "dev.filters.region"},
            }
        ],
    )
    resources = table.additional_core_resources
    tags = [r for r in resources if isinstance(r, EntityTagAssignment)]
    table_id = "${resources.table-dev-markets-slv_trades.id}"
    assert [(t.entity_type, t.entity_name, t.tag_key) for t in tags] == [
        ("tables", table_id, "data_classification"),
        ("columns", f"{table_id}.trader_email", "pii"),
    ]
    (policy,) = [r for r in resources if isinstance(r, PolicyInfo)]
    assert policy.on_securable_type == "TABLE"
    assert policy.on_securable_fullname == table_id

    # Column tags are not part of the table definition
    props = table.terraform_properties
    assert "tags" not in props
    assert props["column"][1] == {"name": "trader_email", "type": "string"}


def test_terraform_plan():
    skip_terraform_plan()
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        table_type="MANAGED",
    )
    plan_resource(table)
