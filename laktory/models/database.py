from pydantic import computed_field

from laktory.models.base import BaseModel
from laktory.models.table import Table


class Database(BaseModel):
    name: str
    comment: str = None
    tables: list[Table] = []
    parent_id: str = None

    @computed_field
    @property
    def catalog_name(self) -> str:
        if self.parent_id is None or len(self.parent_id.split(".")) < 1:
            return None
        return self.parent_id.split(".")[-1]
