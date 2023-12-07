from laktory.models.basemodel import BaseModel


class Secret(BaseModel):
    scope: str = None
    key: str = None
    value: str = None
