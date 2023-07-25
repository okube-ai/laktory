from laktory.models import EventDefinition
from laktory.models import EventData
from laktory.models import Producer


class StockPriceDefinition(EventDefinition):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")
    landing_mount_path: str = ""


class StockPriceData(EventData):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")
    landing_mount_path: str = ""
