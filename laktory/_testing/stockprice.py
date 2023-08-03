from laktory.models import EventDefinition
from laktory.models import EventData
from laktory.models import Producer
from laktory.models import Pipeline
from laktory.models import Table


class StockPriceDefinition(EventDefinition):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")
    landing_mount_path: str = ""


class StockPriceData(EventData):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")
    landing_mount_path: str = ""


class StockPricesPipeline(Pipeline):
    name: str = "pl-stock-prices"
    tables: list[Table] = [
        Table(**{
            "name": "brz_stock_prices",
            "input_event": "yahoo-finance/stock_price",
            "zone": "BRONZE",
        }),
        Table(**{
            "name": "slv_stock_prices",
            "input_table": "brz_stock_prices",
            "zone": "SILVER",
            "columns": [
            ]
        }),
    ]
