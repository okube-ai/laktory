import pulumi
import os

from laktory.models import BaseModel
from laktory.models import Table
from laktory.models import Schema
from laktory import settings
from pulumi_random import RandomString

dirpath = os.path.dirname(__file__)

env = RandomString("env", length=3, upper=False, numeric=False, special=False)
schema_name = RandomString(
    "schema", length=5, upper=False, numeric=False, special=False
)


schema = Schema(
    name="${vars.env}.${vars.schema_name}",
    catalog_name="${vars.env}",
    tables=[
        Table(
            name="AAPL",
            columns=[
                {
                    "name": "open",
                    "type": "double",
                    "spark_func_kwargs": {
                        "window_length": 2
                    },  # window_length should not be converted to camel
                },
                {
                    "name": "close",
                    "type": "double",
                },
            ],
        ),
        Table(
            name="GOOGL",
            columns=[
                {
                    "name": "${vars.dynamic_column}",
                    "type": "double",
                },
                {
                    "name": "high",
                    "type": "double",
                },
            ],
        ),
    ],
    variables={
        "dynamic_column": "low",
        "env": env.id,
        "schema_name": schema_name.id,
    },
)


def test_read_yaml():
    class OHLC(BaseModel):
        open: float = None
        high: float = None
        low: float = None
        close: float = None

    class Price(BaseModel):
        timestamp: float
        ohlc: OHLC

    class StockPrices(BaseModel):
        symbol: str
        prices: list[Price]

    with open(os.path.join(dirpath, "stockprices0.yaml"), "r") as fp:
        stockprices = StockPrices.model_validate_yaml(fp)

    assert stockprices.model_dump() == {
        "symbol": "AAPL",
        "prices": [
            {
                "timestamp": 1.0,
                "ohlc": {"open": 0.0, "high": None, "low": None, "close": 1.0},
            },
            {
                "timestamp": 2.0,
                "ohlc": {"open": 1.0, "high": None, "low": None, "close": 2.0},
            },
            {
                "timestamp": 3.0,
                "ohlc": {"open": None, "high": None, "low": 3.0, "close": 4.0},
            },
        ],
    }


def test_camelize():
    settings.camel_serialization = True
    dump = schema.model_dump()
    print(dump)
    assert dump == {
        "comment": None,
        "grants": None,
        "name": "${vars.env}.${vars.schema_name}",
        "tables": [
            {
                "builder": {
                    "aggregation": None,
                    "filter": None,
                    "joins": [],
                    "layer": None,
                    "selects": None,
                    "template": None,
                    "dropColumns": [],
                    "dropDuplicates": None,
                    "dropSourceColumns": None,
                    "eventSource": None,
                    "joinsPostAggregation": [],
                    "pipelineName": None,
                    "tableSource": None,
                    "windowFilter": None,
                },
                "columns": [
                    {
                        "comment": None,
                        "name": "open",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "schemaName": None,
                        "sparkFuncArgs": [],
                        "sparkFuncKwargs": {
                            "window_length": {
                                "value": 2,
                                "isColumn": False,
                                "toLit": True,
                                "toExpr": False,
                            }
                        },
                        "sparkFuncName": None,
                        "sqlExpression": None,
                        "tableName": "AAPL",
                    },
                    {
                        "comment": None,
                        "name": "close",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "schemaName": None,
                        "sparkFuncArgs": [],
                        "sparkFuncKwargs": {},
                        "sparkFuncName": None,
                        "sqlExpression": None,
                        "tableName": "AAPL",
                    },
                ],
                "comment": None,
                "data": None,
                "expectations": [],
                "grants": None,
                "name": "AAPL",
                "catalogName": "${vars.env}",
                "dataSourceFormat": "DELTA",
                "primaryKey": None,
                "schemaName": "${vars.env}.${vars.schema_name}",
                "tableType": "MANAGED",
                "timestampKey": None,
                "viewDefinition": None,
                "warehouseId": "08b717ce051a0261",
            },
            {
                "builder": {
                    "aggregation": None,
                    "filter": None,
                    "joins": [],
                    "layer": None,
                    "selects": None,
                    "template": None,
                    "dropColumns": [],
                    "dropDuplicates": None,
                    "dropSourceColumns": None,
                    "eventSource": None,
                    "joinsPostAggregation": [],
                    "pipelineName": None,
                    "tableSource": None,
                    "windowFilter": None,
                },
                "columns": [
                    {
                        "comment": None,
                        "name": "${vars.dynamic_column}",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "schemaName": None,
                        "sparkFuncArgs": [],
                        "sparkFuncKwargs": {},
                        "sparkFuncName": None,
                        "sqlExpression": None,
                        "tableName": "GOOGL",
                    },
                    {
                        "comment": None,
                        "name": "high",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "schemaName": None,
                        "sparkFuncArgs": [],
                        "sparkFuncKwargs": {},
                        "sparkFuncName": None,
                        "sqlExpression": None,
                        "tableName": "GOOGL",
                    },
                ],
                "comment": None,
                "data": None,
                "expectations": [],
                "grants": None,
                "name": "GOOGL",
                "catalogName": "${vars.env}",
                "dataSourceFormat": "DELTA",
                "primaryKey": None,
                "schemaName": "${vars.env}.${vars.schema_name}",
                "tableType": "MANAGED",
                "timestampKey": None,
                "viewDefinition": None,
                "warehouseId": "08b717ce051a0261",
            },
        ],
        "volumes": [],
        "catalogName": "${vars.env}",
        "forceDestroy": True,
    }

    settings.camel_serialization = False


def test_inject_vars():
    d0 = schema.model_dump()
    d1 = schema.inject_vars(d0)
    assert d1["tables"][-1]["columns"][0]["name"] == "low"
    assert isinstance(d1["name"], pulumi.Output)
    assert isinstance(d1["catalog_name"], pulumi.Output)


if __name__ == "__main__":
    test_read_yaml()
    test_camelize()
    test_inject_vars()
