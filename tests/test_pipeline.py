from laktory.models import Pipeline
from laktory._testing import StockPriceDefinition

event = StockPriceDefinition()


def test_pipeline():
    pl = Pipeline(
        name="stocks",
        tables=[
            {
                "name": "brz_stocks",
                "event": {
                    "name": event.name,
                    "producer": {
                        "name": event.producer.name,
                    },
                },
                "zone": "BRONZE",
            },
        ]
    )
    assert pl.tables[0].zone == "BRONZE"
    assert pl.model_dump()["tables"][0]["zone"] == "BRONZE"


if __name__ == "__main__":
    test_pipeline()
