from laktory.models.base import BaseModel
from laktory.models.producer import Producer
from laktory.models.ingestion_pattern import IngestionPattern


class DataEvent(BaseModel):
    name: str
    description: str = None
    producer: Producer = None
    ingestion_pattern: IngestionPattern = IngestionPattern()
    data: dict = {}
    # tstamp column
