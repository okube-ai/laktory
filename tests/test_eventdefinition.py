from laktory.models import EventDefinition

event = EventDefinition(
    name="flight_record",
    description="A single data point in a flight test recoding",
    producer={
        "name": "FDR"
    },
    ingestion_pattern={
        "source": "STORAGE_EVENTS",
    },
)


def test_eventdefinition():
    print(event)
    assert event.producer.name == "FDR"
    assert event.ingestion_pattern.source == "STORAGE_EVENTS"


if __name__ == "__main__":
    test_eventdefinition()
