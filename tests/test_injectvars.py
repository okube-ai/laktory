import os

from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks import Schema
from laktory._testing import Paths

paths = Paths(__file__)

env = "dev"
schema_name = "schema"


schema = Schema(
    name="${vars.env}.${vars.schema_name}",
    catalog_name="${vars.env}",
    tables=[
        Table(
            name="AAPL",
            columns=[
                {
                    "name": "${resources.close.id}",
                    "type": "double",
                },
                {
                    "name": "${vars.env}",
                    "type": "string",
                },
                {
                    "name": "${vars.var1}",
                    "type": "string",
                },
                {
                    "name": "${vars.var1_}",
                    "type": "double",
                },
                {
                    "name": "${vars.var2}",
                    "type": "double",
                },
                {
                    "name": "${vars.var3}",
                    "type": "double",
                },
            ],
        ),
    ],
    variables={
        "env": env,
        "schema_name": schema_name,
        r"\$\{resources\.([\w.]+)\}": r"${ \1 }",
        "var1": "${vars.VAR1}",
        "var1_": "${vars.VAR1}_",
        "var2": "${vars.VAR_TWO}",
        "var3": "${vars.var1_}",
    },
)


def test_inject_stack_vars():

    schema2 = schema.inject_vars(inplace=False)
    col_names = [c.name for c in schema2.tables[0].columns]

    # Test values
    assert schema2.name == "dev.schema"
    assert schema2.catalog_name == "dev"
    assert col_names == [
        "${ close.id }",
        "dev",
        "${vars.VAR1}",
        "${vars.VAR1}_",
        "${vars.VAR_TWO}",
        "${vars.VAR1}_",
    ]

    # Test inplace
    schema3 = schema.model_copy(deep=True)
    # schema2 = schema.inject_vars()
    schema3.inject_vars(inplace=True)
    assert schema3.model_dump(exclude_unset=True) == schema2.model_dump(
        exclude_unset=True
    )

    # Test dump
    d0 = schema.model_dump(exclude_unset=True)
    d1 = schema.inject_vars_into_dump(d0, inplace=False)
    schema.inject_vars_into_dump(d0, inplace=True)
    d2 = schema2.model_dump(exclude_unset=True)
    assert d0 == d2
    assert d1 == d2


def test_inject_env_vars():

    env_vars = {
        "ENV": "PROD",
        "VAR1": "VAR1",
        "VAR_TWO": "VAR2",
    }

    for k, v in env_vars.items():
        os.environ[k] = v

    schema2 = schema.inject_vars(inplace=False)
    col_names = [c.name for c in schema2.tables[0].columns]

    # Test values
    assert schema2.name == "dev.schema"
    assert schema2.catalog_name == "dev"
    assert col_names == ["${ close.id }", "dev", "VAR1", "VAR1_", "VAR2", "VAR1_"]

    # Reset
    for k in env_vars:
        del os.environ[k]


if __name__ == "__main__":
    test_inject_stack_vars()
    test_inject_env_vars()
