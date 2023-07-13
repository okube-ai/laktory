import pytest
from pydantic import ValidationError

from laktory.models import Column
from laktory.models import Table


def test_model():
    table = Table(
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
    )

    assert table.columns == [
        Column(name="airspeed", type="double"),
        Column(name="altitude", type="double"),
    ]
    assert table.catalog_name == "lakehouse"
    assert table.schema_name == "flights"
    assert table.zone == "SILVER"

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="f0001", zone="ROUGE")


if __name__ == "__main__":
    test_model()
