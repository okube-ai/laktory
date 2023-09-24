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
    # Container Client                                                        #
    # ----------------------------------------------------------------------- #

    container = ContainerClient.from_connection_string(
        conn_str=os.getenv("LAKTORY_SA_CONN_STR"),
        container_name="landing",
    )

    # ----------------------------------------------------------------------- #
    # Fetch events                                                            #
    # ----------------------------------------------------------------------- #

    events = []
    for s in symbols:
        df = yf.download(s, t0, t1, interval="1m")
        for _, row in df.iterrows():
            events += [StockPriceData(
                data={
                    "created_at": _,
                    "symbol": s,
                    "open": float(row["Open"]),  # np.float64 are not supported for serialization
                    "close": float(row["Close"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                },
            )]

    # --------------------------------------------------------------------------- #
    # Write events                                                                #
    # --------------------------------------------------------------------------- #

    for event in events:
        suffix = event.data["symbol"].lower()
        path = event.get_filepath(suffix=suffix)
        blob = container.get_blob_client(path)
        if not blob.exists():
            blob.upload_blob(
                event.model_dump_json(),
                overwrite=False,
            )
