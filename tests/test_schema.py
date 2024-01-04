from datetime import datetime

from laktory.models import Table
from laktory.models import Schema
from laktory.models import Catalog
from laktory.models import Column


def test_model():
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

    assert schema.tables[0].columns[0].name == "airspeed"
    assert type(schema.tables[0].columns[0]) == Column
    assert schema.name == "flights"
    assert schema.full_name == "laktory_testing.flights"


if __name__ == "__main__":
    test_model()
