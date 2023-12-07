import pulumi

from laktory.models import Table
from laktory.models import Schema
from pulumi_random import RandomString

env = RandomString("env", length=3, upper=False, numeric=False, special=False)
schema_name = RandomString(
    "schema", length=5, upper=False, numeric=False, special=False
)


schema = Schema(
    name="${var.env}.${var.schema_name}",
    catalog_name="${var.env}",
    tables=[
        Table(
            name="AAPL",
            columns=[
                {
                    "name": "open",
                    "type": "double",
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
                    "name": "${var.dynamic_column}",
                    "type": "double",
                },
                {
                    "name": "high",
                    "type": "double",
                },
            ],
        ),
    ],
    vars={
        "dynamic_column": "low",
        "env": env.id,
        "schema_name": schema_name.id,
    },
)


def test_inject_vars():
    d0 = schema.model_dump()
    d1 = schema.inject_vars(d0)
    assert d1["tables"][-1]["columns"][0]["name"] == "low"
    assert isinstance(d1["name"], pulumi.Output)
    assert isinstance(d1["catalog_name"], pulumi.Output)


def test_pulumi_dump():
    d = schema.model_pulumi_dump()
    del d["name"]
    del d["catalog_name"]
    assert d == {"comment": None, "force_destroy": True}


if __name__ == "__main__":
    test_inject_vars()
    test_pulumi_dump()
