from typing import Union
from pydantic import computed_field

from laktory import settings
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.schema import Schema
from laktory.models.grants.cataloggrant import CatalogGrant


class Catalog(BaseModel, Resources):
    name: str
    comment: Union[str, None] = None
    schemas: list[Schema] = []
    is_unity: bool = True
    grants: list[CatalogGrant] = None

    # Deployment options
    owner: str = None
    force_destroy: bool = True
    storage_root: str = None
    isolation_mode: str = "OPEN"

    def model_post_init(self, __context):
        for schema in self.schemas:
            schema.catalog_name = self.name
            schema.model_post_init(None)

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

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

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, opts=None):
        from laktory.resourcesengines.pulumi.catalog import PulumiCatalog
        return PulumiCatalog(name=name, catalog=self, opts=opts)
