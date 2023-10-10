from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.grants.volumegrant import VolumeGrant


class Volume(BaseModel, Resources):
    name: str
    catalog_name: str = None
    schema_name: str = None
    volume_type: str = "MANAGED"
    storage_location: str = None
    grants: list[VolumeGrant] = None

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.schema_name:
            if _id == "":
                _id = self.schema_name
            else:
                _id += f".{self.schema_name}"

        return _id

    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, **kwargs):
        from laktory.resourcesengines.pulumi.volume import PulumiVolume
        return PulumiVolume(name=name, volume=self, **kwargs)
