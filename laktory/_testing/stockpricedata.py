import os
from datetime import datetime

from laktory.models import DataEvent
from laktory.models import Producer


class StockPriceData(DataEvent):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")


if __name__ == "__main__":
    import yfinance as yf
    from azure.storage.blob import ContainerClient

    # ----------------------------------------------------------------------- #
    # Build and publish data for testing                                      #
    # ----------------------------------------------------------------------- #

    symbols = [
        "AAPL",
        "AMZN",
        "GOOGL",
        "MSFT",
    ]

    t0 = datetime(2023, 9, 1)
    t1 = datetime(2023, 9, 4)

    # ----------------------------------------------------------------------- #
    # Fetch events                                                            #
    # ----------------------------------------------------------------------- #

    events = []
    for s in symbols:
        df = yf.download(s, t0, t1, interval="1m")
        for _, row in df.iterrows():
            events += [
                StockPriceData(
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

    # --------------------------------------------------------------------------- #
    # Write events                                                                #
    # --------------------------------------------------------------------------- #

    for event in events:
        event.to_azure_storage_container(skip_if_exists=True)
