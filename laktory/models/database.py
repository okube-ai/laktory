from typing import Union
from pydantic import computed_field

from laktory.models.base import BaseModel
from laktory.models.table import Table


class Database(BaseModel):
    name: str
    comment: Union[str, None] = None
    tables: list[Table] = []
    catalog_name: Union[str, None] = None

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @computed_field
    @property
    def parent_full_name(self) -> str:
        return self.catalog_name

    @computed_field
    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def exists(self):
        return self.name in [c.name for c in self.workspace_client.schemas.list(catalog_name=self.catalog_name)]

    def create(self, if_not_exists: bool = True):

        w = self.workspace_client
        exists = self.exists()

        if if_not_exists and exists:
            return w.schemas.get(self.full_name)

        return w.schemas.create(
            name=self.name,
            catalog_name=self.catalog_name,
            comment=self.comment,
        )

    def delete(self):
        self.workspace_client.schemas.delete(self.full_name)
