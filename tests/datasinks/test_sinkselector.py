from laktory.models import BaseModel
from laktory.models import HiveMetastoreDataSink
from laktory.models import UnityCatalogDataSink
from laktory.models.datasinks import DataSinksUnion


class Sink(BaseModel):
    sink: DataSinksUnion


def test_selector():
    sink = Sink(
        sink={
            "catalog_name": "c",
            "schema_name": "s",
            "table_name": "t",
        }
    )
    assert isinstance(sink.sink, UnityCatalogDataSink)

    sink = Sink(
        sink={
            "catalog_name": "c",
            "schema_name": "s",
            "table_name": "t",
            "type": "HIVE_METASTORE",
        }
    )
    assert isinstance(sink.sink, HiveMetastoreDataSink)
