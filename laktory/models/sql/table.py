from typing import Any
from typing import Union

from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.sql.column import Column
from laktory.models.compute.tablebuilder import TableBuilder
from laktory.models.grants.tablegrant import TableGrant

logger = get_logger(__name__)


class Table(BaseModel, Resources):
    catalog_name: Union[str, None] = None
    columns: list[Column] = []
    data_source_format: str = "DELTA"
    table_type: str = "MANAGED"
    comment: Union[str, None] = None
    data: list[list[Any]] = None
    grants: list[TableGrant] = None
    name: str
    primary_key: Union[str, None] = None
    schema_name: Union[str, None] = None
    timestamp_key: Union[str, None] = None
    builder: TableBuilder = TableBuilder()

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    @model_validator(mode="after")
    def assign_catalog_schema(self) -> Any:
        # Assign to columns
        for c in self.columns:
            c.table_name = self.name
            c.catalog_name = self.catalog_name
            c.schema_name = self.schema_name

        # Set builder table
        self.builder._table = self

        # Assign to sources
        if self.builder.table_source is not None:
            if self.builder.table_source.catalog_name is None:
                self.builder.table_source.catalog_name = self.catalog_name
            if self.builder.table_source.schema_name is None:
                self.builder.table_source.schema_name = self.schema_name

        # Assign to joins
        for join in self.builder.joins:
            if join.other.catalog_name is None:
                join.other.catalog_name = self.catalog_name
            if join.other.schema_name is None:
                join.other.schema_name = self.schema_name

        return self

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

    @property
    def database_name(self) -> str:
        return self.schema_name

    @property
    def layer(self):
        return self.builder.layer

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def column_names(self):
        return [c.name for c in self.columns]

    @property
    def df(self):
        import pandas as pd

        return pd.DataFrame(data=self.data, columns=self.column_names)

    def to_df(self, spark=None):
        import pandas as pd

        df = pd.DataFrame(data=self.data, columns=self.column_names)

        if spark:
            df = spark.createDataFrame(df)
        return df

    @property
    def is_from_cdc(self):
        return self.builder.is_from_cdc

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["builder", "columns", "data", "grants", "primary_key", "timestamp_key"]

    def model_pulumi_dump(self, *args, **kwargs):
        d = super().model_pulumi_dump(*args, **kwargs)
        d["columns"] = []
        for i, c in enumerate(self.columns):
            d["columns"] += [{
                "name": c.name,
                "comment": c.comment,
                "type": c.type,
            }]
        return d

    def deploy_with_pulumi(self, name=None, opts=None):
        from laktory.resourcesengines.pulumi.table import PulumiTable

        return PulumiTable(name=name, table=self, opts=opts)
