from laktory.models.base import BaseModel


class Secret(BaseModel):
    scope: str = None
    key: str = None
    value: str = None
