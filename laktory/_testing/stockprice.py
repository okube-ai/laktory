from laktory.models import EventData
from laktory.models import Producer
from laktory.models import Pipeline
from laktory.models import Table
from laktory.models import EventSource


class StockPriceSource(EventSource):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")


class StockPriceData(EventData):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")
    # landing_mount_path: str = ""


class StockPricesPipeline(Pipeline):
    name: str = "pl-stock-prices"
    tables: list[Table] = [
        Table(**{
            "name": "brz_stock_prices",
            "event_source": StockPriceSource(),
            "zone": "BRONZE",
        }),
        Table(**{
            "name": "slv_stock_prices",
            "table_source": {"name": "brz_stock_prices"},
            "zone": "SILVER",
            "columns": [
            ]
        }),
    ]
