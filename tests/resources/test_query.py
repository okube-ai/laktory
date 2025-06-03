from laktory.models.resources.databricks import Query

query = Query(
    display_name="google-prices",
    parent_path="/queries",
    query_text="SELECT * FROM dev.finance.slv_stock_prices",
    warehouse_id="12345",
)


def test_query():
    assert query.display_name == "google-prices"


if __name__ == "__main__":
    test_query()
