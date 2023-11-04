from laktory.models import DataEventHeader
from laktory.models import DataEvent
from laktory.models import Producer
from datetime import datetime


class StockPriceDataEventHeader(DataEventHeader):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")


class StockPriceDataEvent(StockPriceDataEventHeader, DataEvent):
    pass


class EventsManager:

    def __init__(self):
        self.events = []

    def build_events(self, events_root: str = None):

        import yfinance as yf

        symbols = [
            "AAPL",
            "AMZN",
            "GOOGL",
            "MSFT",
        ]

        t0 = datetime(2023, 9, 1)
        t1 = datetime(2023, 9, 30)

        self.events = []
        for s in symbols:
            df = yf.download(s, t0, t1, interval="1d")
            for _, row in df.iterrows():
                self.events += [
                    StockPriceDataEvent(
                        data={
                            "created_at": _,
                            "symbol": s,
                            "open": float(
                                row["Open"]
                            ),  # np.float64 are not supported for serialization
                            "close": float(row["Close"]),
                            "high": float(row["High"]),
                            "low": float(row["Low"]),
                        },
                    )
                ]
                if events_root:
                    self.events[-1].events_root = events_root

        return self.events

    def to_path(self):
        for event in self.events:
            event.to_path(suffix=event.data["symbol"], skip_if_exists=True)

    def to_azure_storage(self):
        for event in self.events:
            event.to_azure_storage_container(skip_if_exists=True)


if __name__ == "__main__":

    manager = EventsManager()
    manager.build_events(events_root="./events/")
    manager.to_path()
    # manager.to_azure_storage()

