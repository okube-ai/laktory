from laktory.models.base import BaseModel
from laktory.models.grants.volumegrant import VolumeGrant


class Volume(BaseModel):
    name: str
    catalog_name: str = None
    database_name: str = None
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

        if self.database_name:
            if _id == "":
                _id = self.database_name
            else:
                _id += f".{self.database_name}"

        return _id

    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id