from laktory.models import DataEvent
from datetime import datetime

event = DataEvent(
    name="flight_record",
    description="A single data point in a flight test recoding",
    producer={
        "name": "FDR"
    },
    ingestion_pattern={
        "source": "STORAGE_EVENTS",
    },
    data={
        "tstamp": datetime(2023, 7, 1, 1, 0, 0),
        "airspeed": 100.0,
        "altitude": 20000.0,
    }
)


def test_dataevent():
    print(event)
    assert event.producer.name == "FDR"
    assert event.ingestion_pattern.source == "STORAGE_EVENTS"
    assert event.data["altitude"] == 20000


if __name__ == "__main__":
    test_dataevent()
