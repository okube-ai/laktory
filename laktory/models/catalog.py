from typing import Union
from pydantic import computed_field

from laktory import settings
from laktory.models.base import BaseModel
from laktory.models.database import Database


class Catalog(BaseModel):
    name: str
    comment: Union[str, None] = None
    databases: list[Database] = []
    is_unity: bool = True

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @computed_field
    @property
    def full_name(self) -> str:
        return self.name

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def exists(self):
        return self.name in [c.name for c in self.workspace_client.catalogs.list()]

    def create(self, if_not_exists: bool = True):

        w = self.workspace_client
        exists = self.exists()

        if if_not_exists and exists:
            return w.catalogs.get(self.name)

        return w.catalogs.create(
            name=self.name,
            comment=self.comment,
        )

    def delete(self, force: bool = False):
        self.workspace_client.catalogs.delete(self.name, force=force)
