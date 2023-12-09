import os
from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from pydantic import Field
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.permission import Permission
from laktory.models.databricks.cluster import Cluster
from laktory.models.sql.table import Table


class PipelineLibraryFile(BaseModel):
    path: str


class PipelineLibraryNotebook(BaseModel):
    path: str


class PipelineLibrary(BaseModel):
    file: str = None
    notebook: PipelineLibraryNotebook = None


class PipelineNotifications(BaseModel):
    alerts: list[
        Literal[
            "on-update-success",
            "on-update-failure",
            "on-update-fatal-failure",
            "on-flow-failure",
        ]
    ]
    recipients: list[str]


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


class PipelineUDF(BaseModel):
    module_name: str
    function_name: str
    module_path: str = None


class DLTPipeline(BaseModel, BaseResource):
    allow_duplicate_names: bool = None
    catalog: str = None
    channel: str = "PREVIEW"
    clusters: list[PipelineCluster] = []
    configuration: dict[str, str] = {}
    continuous: bool = None
    development: Union[bool, str] = None
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
    udfs: list[PipelineUDF] = []

    @model_validator(mode="after")
    def assign_pipeline_to_tables(self) -> Any:
        for t in self.tables:
            t.builder.pipeline_name = self.name
            t.catalog_name = self.catalog
            t.schema_name = self.target
            for c in t.columns:
                c.table_name = t.name
                c.catalog_name = t.catalog_name
                c.schema_name = t.schema_name

            # Assign to sources
            if t.builder.table_source is not None:
                if t.builder.table_source.catalog_name is None:
                    t.builder.table_source.catalog_name = self.catalog
                if t.builder.table_source.schema_name is None:
                    t.builder.table_source.schema_name = self.target

        return self

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        return "pipeline"

    @property
    def id(self):
        if self._resources is None:
            return None
        return self.resources.pipeline.id

    @property
    def pulumi_excludes(self) -> list[str]:
        return {
            "permissions": True,
            "tables": True,
            "clusters": {"__all__": {"permissions"}},
            "udfs": True,
        }

    def model_pulumi_dump(self, *args, **kwargs):
        d = super().model_pulumi_dump(*args, **kwargs)
        _clusters = []
        for c in d.get("clusters", []):
            c["label"] = c.pop("name")
            _clusters += [c]
        d["clusters"] = _clusters
        return d

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.pipeline import PulumiPipeline

        return PulumiPipeline(name=name, pipeline=self, opts=opts)
