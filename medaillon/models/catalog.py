from pydantic import computed_field

from medaillon.models.base import BaseModel
from medaillon.models.database import Database


class Catalog(BaseModel):
    name: str
    comment: str = None
    databases: list[Database] = []
    is_unity: bool = True
