import pytest

from laktory.models import DataEvent
from laktory.models import DataEventHeader
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


def test_dataevent_header():
    header = DataEventHeader(
        name="flight_record",
        description=None,
        producer={
            "name": "FDR",
            "party": 1,
        },
    )
    assert header.model_dump() == {
        "name": "flight_record",
        "description": None,
        "producer": {"name": "FDR", "description": None, "party": 1},
        "events_root": "/Volumes/dev/sources/landing/events/",
    }
    assert header.event_root == "/Volumes/dev/sources/landing/events/FDR/flight_record/"


def test_dataevent():
    assert event.producer.name == "FDR"
    assert event.data["altitude"] == 20000
    assert event.data["_producer_name"] == "FDR"
    assert event.data["_created_at"] == datetime(
        2023, 7, 1, 1, 0, 0, tzinfo=ZoneInfo("UTC")
    )
    assert event.created_at == datetime(2023, 7, 1, 1, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert (
        event.get_landing_filepath()
        == "/Volumes/dev/sources/landing/events/FDR/flight_record/2023/07/01/flight_record_20230701T010000000Z.json"
    )


def test_model_dump():
    # Without exclusions
    d = event.model_dump(exclude=[])
    print(d)
    for k, v in d.items():
        print(k, v)
    assert d == {
        "event_name": "flight_record",
        "event_description": None,
        "event_producer": {"name": "FDR", "description": None, "party": 1},
        "events_root": "/Volumes/dev/sources/landing/events/",
        "data": {
            "created_at": "2023-07-01T01:00:00",
            "airspeed": 100.0,
            "altitude": 20000.0,
            "_name": "flight_record",
            "_producer_name": "FDR",
            "_created_at": "2023-07-01T01:00:00Z",
        },
        "tstamp_col": "created_at",
    }

    # With exclusions
    d = event.model_dump()
    print(d.keys())
    assert list(d.keys()) == [
        "event_name",
        "event_description",
        "event_producer",
        "data",
    ]
    # for k, v in d.items():
    #     print(k, v)


def test_to_azure_storage_container():
    try:
        import azure.storage
    except ModuleNotFoundError:
        return

    event.to_azure_storage_container(container_name="unit-testing", overwrite=True)
    with pytest.raises(FileExistsError):
        event.to_azure_storage_container(container_name="unit-testing")
    event.to_azure_storage_container(container_name="unit-testing", skip_if_exists=True)


def test_to_aws_s3_bucket():
    try:
        import boto3
    except ModuleNotFoundError:
        return

    event.to_aws_s3_bucket(bucket_name="okube-unit-testing", overwrite=True)
    with pytest.raises(FileExistsError):
        event.to_aws_s3_bucket(bucket_name="okube-unit-testing")
    event.to_aws_s3_bucket(bucket_name="okube-unit-testing", skip_if_exists=True)


def test_to_databricks_mount():
    # TODO: add tests
    pass
    # event.to_databricks_mount()


if __name__ == "__main__":
    test_dataevent_header()
    test_dataevent()
    test_model_dump()
    test_to_azure_storage_container()
    test_to_aws_s3_bucket()
    test_to_databricks_mount()
