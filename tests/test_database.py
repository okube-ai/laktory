from laktory.models import Table
from laktory.models import Database
from laktory.models import Column


def test_model():
    db = Database(
        name="flights",
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
                zone="SILVER",
                parent_id="lakehouse.flights",
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
                zone="SILVER",
                parent_id="lakehouse.flights",
            ),
        ],
    )

    print(db.tables[0].columns[0].name)

    assert db.tables[0].columns[0].name == "airspeed"
    assert type(db.tables[0].columns[0]) == Column


if __name__ == "__main__":
    test_model()
