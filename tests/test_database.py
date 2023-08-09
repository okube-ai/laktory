from laktory.models import Table
from laktory.models import Database
from laktory.models import Catalog
from laktory.models import Column


def test_model():
    db = Database(
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
                zone="SILVER",
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
            ),
        ],
    )

    assert db.tables[0].columns[0].name == "airspeed"
    assert type(db.tables[0].columns[0]) == Column
    assert db.name == "flights"
    assert db.full_name == "laktory_testing.flights"


def test_create():
    cat = Catalog(name="laktory_testing",)
    cat.create(if_not_exists=True)
    db = Database(name="default", catalog_name="laktory_testing")
    db.create()
    assert db.exists()
    db.delete(force=True)
    assert not db.exists()
    cat.delete(force=True)


if __name__ == "__main__":
    test_model()
    test_create()
