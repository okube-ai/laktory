# Laktory

[![pypi](https://img.shields.io/pypi/v/laktory.svg)](https://pypi.org/project/laktory/)
[![downloads](https://static.pepy.tech/badge/laktory/month)](https://pepy.tech/project/laktory)
[![versions](https://img.shields.io/pypi/pyversions/laktory.svg)](https://github.com/okube-ai/laktory)
[![license](https://img.shields.io/github/license/okube-ai/laktory.svg)](https://github.com/okube-ai/laktory/blob/main/LICENSE)

A DataOps framework for building Databricks lakehouse.

## Okube Company 

Okube is committed to develop open source data and ML engineering tools. This is an open space. Contributions are more than welcome.


## Help
TODO: Build full help documentation

## Installation
Install using `pip install laktory`

TODO: Full installation instructions

## A Basic Example
This example demonstrates how to send data events to a data lake and to set a
data pipeline defining the tables transformation layers. 

### Generate data events
A data event class defines specifications of an event and provides the methods
for writing that event to a databricks mount or a cloud storage.

```py
from laktory import models
from datetime import datetime


events = [
    models.DataEvent(
        name="stock_price",
        producer={
            "name": "yahoo-finance",
        },
        data={
            "created_at": datetime(2023, 8, 23),
            "symbol": "GOOGL",
            "open": 130.25,
            "close": 132.33,
        },
    ),
    models.DataEvent(
        name="stock_price",
        producer={
            "name": "yahoo-finance",
        },
        data={
            "created_at": datetime(2023, 8, 24),
            "symbol": "GOOGL",
            "open": 132.00,
            "close": 134.12,
        },
    )
]

for event in events:
    event.to_databricks_mount()

```
These events may now be sent to your cloud storage of choice.

### Define data pipeline and data tables
A pipeline class defines the transformations of a raw data event into curated
(silver) and consumption (gold) layers.

```py
from laktory import models

pl = models.Pipeline(
    name="pl-stock-prices",
    tables=[
        models.Table(
            name="brz_stock_prices",
            timestamp_key="data.created_at",
            event_source=models.EventDataSource(
                name="stock_price",
                producer=models.Producer(
                    name="yahoo-finance",
                )
            ),
            zone="BRONZE",
        ),
        models.Table(
            name="brz_stock_prices",
            table_source=models.TableSource(
                name="brz_stock_prices",
            ),
            zone="SILVER",
            columns = [
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
            ]
        ),
    ]
)
```
Laktory will provide the required framework for deploying this pipeline as a 
delta live tables in Databricks and all the associated notebooks and jobs. 
TODO: link to help


## Contributing
TODO
