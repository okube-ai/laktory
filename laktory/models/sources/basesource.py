from laktory.models.base import BaseModel


class BaseSource(BaseModel):
    read_as_stream: bool | None = True

