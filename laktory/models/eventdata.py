from datetime import datetime
from laktory.models.base import BaseModel


class EventData(BaseModel):
    name: str
    data: dict
    producer_name: str
    created: datetime = datetime.utcnow()
    published: datetime = None
