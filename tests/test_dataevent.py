from laktory.models import DataEvent
from datetime import datetime
from zoneinfo import ZoneInfo

event = DataEvent(
    name="flight_record",
    producer={
        "name": "FDR",
    },
    data={
        "created_at": datetime(2023, 7, 1, 1, 0, 0),
        "airspeed": 100.0,
        "altitude": 20000.0,
    },
)


def test_dataevent():
    assert event.producer.name == "FDR"
    assert event.data["altitude"] == 20000
    assert event.data["_producer_name"] == "FDR"
    assert event.data["_created_at"] == datetime(
        2023, 7, 1, 1, 0, 0, tzinfo=ZoneInfo("UTC")
    )
    assert event.created_at == datetime(2023, 7, 1, 1, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert (
        event.get_filepath()
        == "/mnt/landing/events/FDR/flight_record/2023/07/01/flight_record_20230701T010000000Z.json"
    )


def test_model_dump():
    d = event.model_dump()
    print(d)
    for k, v in d.items():
        print(k, v)
    assert d == {
        "name": "flight_record",
        "description": None,
        "producer": {"name": "FDR", "description": None, "party": 1},
        "events_root_path": "/mnt/landing/events/",
        "dirpath": "/mnt/landing/events/FDR/flight_record/",
        "data": {
            "created_at": "2023-07-01T01:00:00",
            "airspeed": 100.0,
            "altitude": 20000.0,
            "_name": "flight_record",
            "_producer_name": "FDR",
            "_created_at": "2023-07-01T01:00:00Z",
        },
    }


if __name__ == "__main__":
    test_dataevent()
    test_model_dump()
