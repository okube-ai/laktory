from laktory.models.base import BaseModel


class Producer(BaseModel):
    name: str
    description: str = None
    party: int = 1  # First, Second and Third party data
                    # ref.: https://blog.hubspot.com/service/first-party-data
