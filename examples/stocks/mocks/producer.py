import os
from datetime import datetime
import yfinance as yf

from azure.storage.blob import ContainerClient
from laktory._testing import StockPriceDefinition

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

symbols = [
    "AAPL",
    "AMZN",
    "GOOGL",
    "MSFT",
]

t0 = datetime(2023, 7, 1)
t1 = datetime(2023, 7, 4)

# --------------------------------------------------------------------------- #
# Container Client                                                            #
# --------------------------------------------------------------------------- #

container = ContainerClient.from_connection_string(
    conn_str=os.getenv("LAKTORY_SA_CONN_STR"),
    container_name="landing",
)


# --------------------------------------------------------------------------- #
# Fetch events                                                                #
# --------------------------------------------------------------------------- #

events = []
for s in symbols:
    df = yf.download(s, t0, t1, interval="1m")
    for _, row in df.iterrows():
        events += [StockPriceDefinition(
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
    path = event.get_landing_filepath(suffix=suffix)
    blob = container.get_blob_client(path)
    if not blob.exists():
        blob.upload_blob(
            event.model_dump_json(),
            overwrite=False,
        )
