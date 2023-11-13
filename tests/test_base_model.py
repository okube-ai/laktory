from laktory.models import Table
from laktory.models import Schema

schema = Schema(
    name="stocks",
    catalog_name="default",
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
    _vars={"dynamic_column": "low"},
)


def test_inject_vars():
    d0 = schema.model_dump()
    d1 = schema.inject_vars(d0)
    assert d1["tables"][-1]["columns"][0]["name"] == "low"


def test_pulumi_dump():
    d = schema.model_pulumi_dump()
    assert d == {
        "name": "stocks",
        "comment": None,
        "catalog_name": "default",
        "force_destroy": True,
    }


if __name__ == "__main__":
    test_inject_vars()
    test_pulumi_dump()
