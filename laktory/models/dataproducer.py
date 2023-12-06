from typing import Union
from typing import Literal

from laktory.models.base import BaseModel


class DataProducer(BaseModel):
    name: str
    description: Union[str, None] = None
    party: Literal[1, 2, 3] = 1  # First, Second and Third party data
    # ref.: https://blog.hubspot.com/service/first-party-data
