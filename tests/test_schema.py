from datetime import datetime

from laktory.models import Table
from laktory.models import Schema
from laktory.models import Catalog
from laktory.models import Column


def test_model():
    db = Schema(
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


def atest_create():
    # Timestamp is included in catalog name to prevent conflicts when running
    # multiple tests in parallel
    catalog_name = "laktory_testing_" + str(datetime.now().timestamp()).replace(".", "")

    cat = Catalog(name=catalog_name)
    cat.create(if_not_exists=True)
    db = Schema(name="default", catalog_name=catalog_name)
    db.create()
    assert db.exists()
    db.delete()
    assert not db.exists()
    cat.delete(force=True)


if __name__ == "__main__":
    test_model()
    # atest_create()
