from datetime import datetime
from zoneinfo import ZoneInfo
import pytest

from laktory._testing import StockPriceDataEventHeader
from laktory._testing import EventsManager
from laktory import models
from laktory import settings

header = StockPriceDataEventHeader()
events = EventsManager().build_events()
event = events[0]


def test_dataeventheader():
    print(header.model_dump())
    assert header.model_dump() == {
        "name": "stock_price",
        "description": None,
        "producer": {"name": "yahoo-finance", "description": None, "party": 1},
        "events_root_": None,
    }
    assert header.events_root == "/Volumes/dev/sources/landing/events/"
    assert (
        header.event_root
        == "/Volumes/dev/sources/landing/events/yahoo-finance/stock_price/"
    )


def test_dataeventheader_root():
    # Custom value for events root
    header = models.DataEventHeader(name="my-event", events_root="test")
    header2 = models.DataEventHeader(
        **header.model_dump(by_alias=True, exclude_none=True)
    )
    assert header.events_root == "test"
    assert header2.events_root == "test"

    # Default value
    header = models.DataEventHeader(name="my-event")
    assert header.events_root == "/Volumes/dev/sources/landing/events/"
    root0 = settings.workspace_landing_root
    header2 = models.DataEventHeader(
        **header.model_dump(by_alias=True, exclude_none=True)
    )
    settings.workspace_landing_root = "custom_root/"
    assert header.events_root == "custom_root/events/"
    assert header2.events_root == "custom_root/events/"
    settings.workspace_landing_root = root0


def test_dataevent():
    assert event.producer.name == "yahoo-finance"
    assert event.data["symbol"] == "AAPL"
    assert event.data["open"] == pytest.approx(189.49, abs=0.01)
    assert event.data["_producer_name"] == "yahoo-finance"
    assert event.data["_created_at"] == datetime(2023, 9, 1, tzinfo=ZoneInfo("UTC"))
    assert event.created_at == datetime(2023, 9, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert (
        event.get_landing_filepath()
        == "/Volumes/dev/sources/landing/events/yahoo-finance/stock_price/2023/09/01/stock_price_20230901T000000000Z.json"
    )


def test_model_dump():
    # Without exclusions
    d = event.model_dump(exclude=[])
    assert d == {
        "name": "stock_price",
        "description": None,
        "producer": {"name": "yahoo-finance", "description": None, "party": 1},
        "events_root": None,
        "data": {
            "created_at": "2023-09-01T00:00:00",
            "symbol": "AAPL",
            "open": 189.49000549316406,
            "close": 189.4600067138672,
            "high": 189.9199981689453,
            "low": 188.27999877929688,
            "@id": "_id",
            "_name": "stock_price",
            "_producer_name": "yahoo-finance",
            "_created_at": "2023-09-01T00:00:00Z",
        },
        "tstamp_col": "created_at",
        "tstamp_in_path": True,
    }
    assert event.events_root == "/Volumes/dev/sources/landing/events/"

    # With exclusions
    d = event.model_dump()
    print(d)
    assert d == {
        "name": "stock_price",
        "description": None,
        "producer": {"name": "yahoo-finance", "description": None, "party": 1},
        "data": {
            "created_at": "2023-09-01T00:00:00",
            "symbol": "AAPL",
            "open": 189.49000549316406,
            "close": 189.4600067138672,
            "high": 189.9199981689453,
            "low": 188.27999877929688,
            "@id": "_id",
            "_name": "stock_price",
            "_producer_name": "yahoo-finance",
            "_created_at": "2023-09-01T00:00:00Z",
        },
    }


def test_event_without_tstamp():
    d = event.model_dump()
    d["tstamp_in_path"] = False
    e = models.DataEvent(**d)
    assert (
        e.get_landing_filepath()
        == "/Volumes/dev/sources/landing/events/yahoo-finance/stock_price/stock_price.json"
    )


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
    test_dataeventheader()
    test_dataeventheader_root()
    test_dataevent()
    test_model_dump()
    test_event_without_tstamp()
    test_to_azure_storage_container()
    test_to_aws_s3_bucket()
    test_to_databricks_mount()
