from pydantic import computed_field

from laktory.models.base import BaseModel
from laktory.models.database import Database


class Catalog(BaseModel):
    name: str
    comment: str = None
    databases: list[Database] = []
    is_unity: bool = True
