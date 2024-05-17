from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks import Schema
from laktory.models.resources.databricks import Column

schema = Schema(
    name="flights",
    catalog_name="laktory_testing",
    tables=[
        Table(
            name="f1549",
            columns=[
                {
                    "name": "airspeed",
                    "type": "double",
                },
                {
                    "name": "altitude",
                    "type": "double",
                },
            ],
        ),
        Table(
            name="f0002",
            columns=[
                {
                    "name": "airspeed",
                    "type": "double",
                },
                {
                    "name": "altitude",
                    "type": "double",
                },
            ],
        ),
    ],
)


def test_model():
    assert schema.tables[0].columns[0].name == "airspeed"
    assert type(schema.tables[0].columns[0]) == Column
    assert schema.name == "flights"
    assert schema.full_name == "laktory_testing.flights"


if __name__ == "__main__":
    test_model()
