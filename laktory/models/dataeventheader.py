from typing import Union
from pydantic import Field

from laktory._settings import settings
from laktory.models.base import BaseModel
from laktory.models.producer import Producer


class DataEventHeader(BaseModel):
    """
    Data Event Header class defines the context (metadata) describing a data event.
    It is generally used to read data from a storage location.

    Attributes
    ----------
    name
        Data event name
    description
        Data event description
    producer
        Data event producer
    events_root
        Root path for all events

    Examples
    ---------
    >>> from laktory import models
    >>> models.DataEventHeader(
    ...     name="stock_price",
    ...     producer={"name": "yahoo-finance"},
    ... )
    DataEventHeader(name='stock_price', description=None, producer=Producer(name='yahoo-finance', description=None, party=1), events_root='/Volumes/dev/sources/landing/events/')
    """
    name: str = Field(...)
    description: Union[str, None] = Field(default=None)
    producer: Producer = Field(None)
    events_root: str = Field(settings.workspace_landing_root + "events/")

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def event_root(self) -> str:
        """
        Root path for the event, defined as `{self.events_roots}/{producer_name}/{event_name}/`

        Returns
        -------
        str
            Event path
        """
        producer = ""
        if self.producer is not None:
            producer = self.producer.name + "/"
        v = f"{self.events_root}{producer}{self.name}/"
        return v
