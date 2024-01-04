import pulumi

from laktory.models import Table
from laktory.models import Schema
from laktory.models import BaseModel
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
    variables={
        "dynamic_column": "low",
        "env": env.id,
        "schema_name": schema_name.id,
    },
)


class Camel(BaseModel):
    d: dict = {}
    l: list = []


def test_inject_vars():
    d0 = schema.model_dump()
    d1 = schema.inject_vars(d0)
    assert d1["tables"][-1]["columns"][0]["name"] == "low"
    assert isinstance(d1["name"], pulumi.Output)
    assert isinstance(d1["catalog_name"], pulumi.Output)


def test_camel_case():
    camel = Camel(
        d={"this_is_a_test": ["value_a", "value_b", {"key_alpha": 0, "keyBeta": 1}]},
        l=["a", "a-b-c", "class_member"],
    )
    d = camel.model_dump(keys_to_camel_case=True)
    print(d)
    assert d == {
        "d": {"thisIsATest": ["value_a", "value_b", {"keyBeta": 1, "keyAlpha": 0}]},
        "l": ["a", "a-b-c", "class_member"],
    }


if __name__ == "__main__":
    test_inject_vars()
    test_camel_case()
