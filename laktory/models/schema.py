from typing import Union
from pydantic import computed_field

from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.table import Table
from laktory.models.volume import Volume
from laktory.models.grants.schemagrant import SchemaGrant


class Schema(BaseModel, Resources):
    name: str
    comment: Union[str, None] = None
    tables: list[Table] = []
    volumes: list[Volume] = []
    catalog_name: Union[str, None] = None
    grants: list[SchemaGrant] = None

    # Deployment options
    force_destroy: bool = True

    def model_post_init(self, __context):
        for table in self.tables:
            table.catalog_name = self.catalog_name
            table.schema_name = self.name
        for volume in self.volumes:
            volume.catalog_name = self.catalog_name
            volume.schema_name = self.name

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        return self.catalog_name

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
        return self.name in [
            c.name
            for c in self.workspace_client.schemas.list(catalog_name=self.catalog_name)
        ]

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

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, opts=None):
        from laktory.resourcesengines.pulumi.schema import PulumiSchema
        return PulumiSchema(name=name, schema=self, opts=opts)
