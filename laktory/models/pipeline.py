from typing import Any
from pydantic import model_validator

from laktory.models.base import BaseModel
from laktory.models.table import Table
from laktory.models.table import Column


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

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    @model_validator(mode="after")
    def assign_pipeline_to_tables(self) -> Any:
        for t in self.tables:
            t.pipeline_name = self.name

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def get_tables_meta(self, catalog_name="main", database_name="laktory") -> Table:

        table = Table.meta_table()
        table.catalog_name = catalog_name
        table.database_name = database_name

        data = []
        for t in self.tables:
            _dump = t.model_dump(mode="json")
            _data = []
            for c in table.column_names:
                _data += [_dump[c]]
            data += [_data]
        table.data = data

        return table

    def get_columns_meta(self, catalog_name="main", database_name="laktory") -> Table:

        table = Column.meta_table()
        table.catalog_name = catalog_name
        table.database_name = database_name

        data = []
        for t in self.tables:
            for c in t.columns:
                _dump = c.model_dump(mode="json")
                _data = []
                for k in table.column_names:
                    _data += [_dump[k]]
                data += [_data]
        table.data = data

        return table
