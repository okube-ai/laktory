from laktory.models import EventData
from datetime import datetime
from datetime import timedelta

event = EventData(
    name="flight_record",
    producer_name="FDR",
    data={
        "tstamp": datetime(2023, 7, 1, 1, 0, 0),
        "airspeed": 100.0,
        "altitude": 20000.0,
    }
)


def test_eventdata():
    print(event)
    assert event.producer_name == "FDR"
    assert event.data["altitude"] == 20000
    assert abs(event.created - datetime.utcnow()) < timedelta(seconds=1)


if __name__ == "__main__":
    test_eventdata()
