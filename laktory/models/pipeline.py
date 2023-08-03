from laktory.models.base import BaseModel
from laktory.models.table import Table


class Pipeline(BaseModel):
    name: str
    clusters: list = []
    development: bool = True
    continuous: bool = False
    channel: str = "PREVIEW"
    photon: bool = False
    libraries: list = []
    catalog: str = "main"
    target: str = "default"

    tables: list[Table] = []

    def tables_dump(self, *args, **kwargs):
        cols = []
        for c in self.tables:
            cols += [
                c.model_dump(*args, **kwargs)
            ]
        return cols
