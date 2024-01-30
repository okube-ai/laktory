from typing import Union
from pydantic import Field
from pydantic import ConfigDict

from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.dataproducer import DataProducer


class DataEventHeader(BaseModel):
    """
    Data Event Header class defines the context (metadata) describing a data
    event. It is generally used to read data from a storage location or to
    build a `EventDataSource`.

    Attributes
    ----------
    name
        Data event name
    description
        Data event description
    producer
        Data event producer
    event_root
        Root path for specific event. Default value: `{settings.workspace_landing_root}/events/my-event`
    events_root
        Root path for all events. Default value: `{settings.workspace_landing_root}/events/`

    Examples
    ---------
    ```python
    from laktory import models

    event = models.DataEventHeader(
        name="stock_price",
        producer={"name": "yahoo-finance"},
    )
    print(event)
    '''
    variables={} name='stock_price' description=None producer=DataProducer(variables={}, name='yahoo-finance', description=None, party=1) events_root_=None event_root_=None
    '''

    print(event.event_root)
    #> /Volumes/dev/sources/landing/events/yahoo-finance/stock_price/
    ```
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    name: str = Field(...)
    description: Union[str, None] = Field(default=None)
    producer: DataProducer = Field(default=None)
    events_root_: Union[str, None] = Field(None, alias="events_root")
    event_root_: Union[str, None] = Field(None, alias="event_root")

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def events_root(self) -> str:
        """Must be computed to dynamically account for settings (env variable at run time)"""
        if self.events_root_:
            return self.events_root_
        return settings.workspace_landing_root + "events/"

    @property
    def event_root(self) -> str:
        """
        Root path for the event. Default path is `{self.events_roots}/{producer_name}/{event_name}/`, but it may
        be overwritten by self.event_root_.

        Returns
        -------
        str
            Event path
        """
        if self.event_root_:
            return self.event_root_

        producer = ""
        if self.producer is not None:
            producer = self.producer.name + "/"
        v = f"{self.events_root}{producer}{self.name}/"
        return v


if __name__ == "__main__":
    from laktory import models

    event = models.DataEventHeader(
        name="stock_price",
        producer={"name": "yahoo-finance"},
    )
    print(event)

    print(event.event_root)
