from laktory.models import DataEvent
from laktory.models import Producer
from laktory.models import Pipeline
from laktory.models import Table
from laktory.models import EventDataSource


class StockPriceSource(EventDataSource):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")


class StockPricesPipeline(Pipeline):
    name: str = "pl-stock-prices"
    tables: list[Table] = [
        Table(
            **{
                "name": "brz_stock_prices",
                "timestamp_key": "data.created_at",
                "event_source": StockPriceSource(),
                "zone": "BRONZE",
            }
        ),
        Table(
            **{
                "name": "slv_stock_prices",
                "table_source": {"name": "brz_stock_prices"},
                "zone": "SILVER",
                "columns": [
                    {
                        "name": "created_at",
                        "type": "timestamp",
                        "func_name": "coalesce",
                        "input_cols": ["_created_at"],
                    },
                    {
                        "name": "low",
                        "type": "double",
                        "func_name": "coalesce",
                        "input_cols": ["data.low"],
                    },
                    {
                        "name": "high",
                        "type": "double",
                        "func_name": "coalesce",
                        "input_cols": ["data.high"],
                    },
                ],
            }
        ),
    ]


if __name__ == "__main__":
    # Publish Metadata
    pl = StockPricesPipeline()
    pl.publish_tables_meta()
    print(pl)

    # TODO: push pipeline with Pulumi
