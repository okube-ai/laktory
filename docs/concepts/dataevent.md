??? "API Documentation"
    [`laktory.models.DataEvent`][laktory.models.DataEvent]<br>

The `DataEvent` model is supporting the event-based architecture described in [Design Principles](../design.md).
They define the metadata (name, producer, timestamp, etc.) as well as the data (payload) of an event.
They can be used both in the context of generating and consuming data.

### Generation
Suppose you have a scheduled- or trigger-based service that fetch stock prices and want to publish them to a storage account, the landing of your lakehouse.

First declare the event and assign it some data
```py
from laktory import models
from datetime import datetime


event = models.DataEvent(
    name="stock_price",
    producer={"name": "yahoo-finance"},
    data={
        "created_at": datetime(2023, 8, 23),
        "symbol": "GOOGL",
        "open": 130.25,
        "close": 132.33,
    },
)
```

Publishing is as simple as
```py
event.to_databricks()
```
In this case, a Databricks Volume is used, but other methods like `event.to_path()` or `to_azure_storage_container()` are also available.

The default path for the event follows the standard convention
> {events_root}/{producer_name}/{event_name}/{year}/{month}/{day}/{event_name}_{year}{month}{day}T{timestamp}.{format}

But you may customize it using environment variable `WORKSPACE_LANDING_ROOT` or `events_root`, `tstamp_in_path` and `suffix` arguments.

### Format
Currently, data events can be stored and read from `JSON`, `CSV` and `PARQUET` formats. More may be added in the future. 