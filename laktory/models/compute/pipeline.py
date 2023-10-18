import os
from typing import Any
from typing import Literal
from pydantic import model_validator
from pydantic import Field
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.permission import Permission
from laktory.models.compute.cluster import Cluster
from laktory.models.table import Table
from laktory.models.column import Column
from laktory.models.schema import Schema
from laktory.models.catalog import Catalog


class PipelineLibraryFile(BaseModel):
    path: str


class PipelineLibraryNotebook(BaseModel):
    path: str


class PipelineLibrary(BaseModel):
    file: str = None
    notebook: PipelineLibraryNotebook = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.PipelineLibraryArgs(**self.model_dump())


class PipelineNotifications(BaseModel):
    alerts: list[Literal["on-update-success", "on-update-failure", "on-update-fatal-failure", "on-flow-failure"]]
    recipients: list[str]

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.PipelineNotificationArgs(**self.model_dump())


class PipelineCluster(Cluster):
    autotermination_minutes: int = Field(None)
    cluster_id: str = Field(None)
    data_security_mode: str = Field(None)
    enable_elastic_disk: bool = Field(None)
    idempotency_token: str = Field(None)
    is_pinned: bool = Field(None)
    libraries: list[Any] = Field(None)
    node_type_id: str = None
    runtime_engine: str = Field(None)
    single_user_name: str = Field(None)
    spark_version: str = Field(None)

    @model_validator(mode="after")
    def excluded_fields(self) -> Any:

        for f in [
            "autotermination_minutes",
            "cluster_id",
            "data_security_mode",
            "enable_elastic_disk",
            "idempotency_token",
            "is_pinned",
            "libraries",
            "runtime_engine",
            "single_user_name",
            "spark_version",
        ]:
            if getattr(self, f, None) not in [None, [], {}]:
                raise ValueError(f"Field {f} should be null")

        return self


class Pipeline(BaseModel, Resources):
    allow_duplicate_names: bool = None
    catalog: str = None
    channel: str = "PREVIEW"
    clusters: list[PipelineCluster] = []
    configuration: dict[str, str] = {}
    continuous: bool = None
    development: bool = None
    edition: str = None
    # filters
    libraries: list[PipelineLibrary] = []
    name: str
    notifications: list[PipelineNotifications] = []
    permissions: list[Permission] = []
    photon: bool = None
    serverless: bool = None
    storage: str = None
    tables: list[Table] = []
    target: str = None

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
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.pipeline import PulumiPipeline
        return PulumiPipeline(name=name, pipeline=self, opts=opts)

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
