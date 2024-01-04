from typing import Union
from typing import Literal

from laktory.models.basemodel import BaseModel


class DataProducer(BaseModel):
    """
    Specifications on the producer of data.

    Attributes
    ----------
    name:
        Name of the data producer
    description:
        Description of the data producer
    party:
        First, second or third party
        ref.: https://blog.hubspot.com/service/first-party-data

    Examples
    --------
    ```py
    from laktory import models

    producer = models.DataProducer(
        name="yahoo-finance",
        description="yfinance offers a threaded and Pythonic way to download market data from Yahoo! finance",
        party=3,
    )
    print(producer)
    '''
    variables={} name='yahoo-finance' description='yfinance offers a threaded and Pythonic way to download market data from Yahoo! finance' party=3
    '''
    ```
    """

    name: str
    description: Union[str, None] = None
    party: Literal[1, 2, 3] = 1
