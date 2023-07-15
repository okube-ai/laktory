from laktory.models import EventData
from datetime import datetime

event = EventData(
    name="flight_record",
    producer_name="FDR",
    data={
        "created": datetime(2023, 7, 1, 1, 0, 0),
        "airspeed": 100.0,
        "altitude": 20000.0,
    }
)


def test_eventdata():
    assert event.producer_name == "FDR"
    assert event.data["altitude"] == 20000
    assert event.data["_producer_name"] == "FDR"
    assert event.data["_created"] == datetime(2023, 7, 1, 1, 0, 0)
    assert event.created == datetime(2023, 7, 1, 1, 0, 0)
    assert event.get_landing_filepath() == "mnt/landing/events/FDR/flight_record//2023/07/01/flight_record_20230701T010000000Z.json"


if __name__ == "__main__":
    test_eventdata()
