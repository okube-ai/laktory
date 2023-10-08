import json
from typing import Any
from pydantic import model_validator

from laktory.models.base import BaseModel
from laktory.models.table import Table
from laktory.models.table import Column
from laktory.models.catalog import Catalog
from laktory.models.schema import Schema


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
            t.catalog_name = self.catalog
            t.schema_name = self.target
            for c in t.columns:
                c.table_name = t.name
                c.catalog_name = t.catalog_name
                c.schema_name = t.schema_name
        return self

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def get_tables_meta(self, catalog_name="main", schema_name="laktory") -> Table:
        table = Table.meta_table()
        table.catalog_name = catalog_name
        table.schema_name = schema_name

        data = []
        for t in self.tables:
            _dump = t.model_dump(mode="json")
            _data = []
            for c in table.column_names:
                _data += [_dump[c]]
            data += [_data]
        table.data = data

        return table

    def get_columns_meta(self, catalog_name="main", schema_name="laktory") -> Table:
        table = Column.meta_table()
        table.catalog_name = catalog_name
        table.schema_name = schema_name

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

    def publish_tables_meta(self, catalog_name="main", schema_name="laktory", init=True):

        # Create catalog
        Catalog(name=catalog_name).create(if_not_exists=True)

        # Create schema
        Schema(name=schema_name, catalog_name=catalog_name).create(if_not_exists=True)

        # Get and create tables
        tables = self.get_tables_meta(
            catalog_name=catalog_name, schema_name=schema_name
        )
        tables.create(or_replace=init, insert_data=True)

        # Get and create tables
        columns = self.get_columns_meta(
            catalog_name=catalog_name, schema_name=schema_name
        )
        columns.create(or_replace=init, insert_data=True)
