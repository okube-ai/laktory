from typing import Literal
from laktory.models.base import BaseModel

SOURCES = (
    "STORAGE_DUMPS",
    "STORAGE_EVENTS",
    "STREAM_KINESIS",
    "STREAM_KAFKA",
)

FORMATS = (
    "JSON",
    "CSV",
    "PARQUET",
)


class IngestionPattern(BaseModel):
    source: Literal[SOURCES] = "STORAGE_EVENTS"
    read_as_stream: bool = True
    fmt: Literal[FORMATS] = "JSON"
    # tstamp column

