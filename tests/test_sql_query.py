import os
from laktory.models import SqlQuery

root_dir = os.path.dirname(__file__)


def test_sql_query():
    query = SqlQuery(
        name="google-prices",
        parent="/queries",
        query="SELECT * FROM dev.finance.slv_stock_prices",
        warehouse_id="12345",
    )
    data = query.model_dump()
    print(query.resource_key)
    assert query.resource_key == "google-prices"
    print(query.resource_name)
    assert query.resource_name == "sql-query-google-prices"
    print(data)
    assert data == {
        "comment": None,
        "data_source_id": None,
        "name": "google-prices",
        "parent": "/queries",
        "permissions": [],
        "query": "SELECT * FROM dev.finance.slv_stock_prices",
        "run_as_role": None,
        "tags": [],
        "warehouse_id": "12345",
    }


if __name__ == "__main__":
    test_sql_query()
