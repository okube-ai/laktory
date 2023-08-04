from typing import Literal

from laktory.models.eventheader import EventHeader
from laktory.models.sources.basesource import BaseSource


TYPES = (
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


class EventSource(BaseSource, EventHeader):
    type: Literal[TYPES] = "STORAGE_EVENTS"
    fmt: Literal[FORMATS] = "JSON"
    # tstamp_col: str = "created_at"
