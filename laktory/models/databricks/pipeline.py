import json
import os
from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from pydantic import Field

from laktory._settings import settings
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel
from laktory.models.databricks.cluster import Cluster
from laktory.models.databricks.accesscontrol import AccessControl
from laktory.models.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.databricks.workspacefile import WorkspaceFile
from laktory.models.sql.table import Table


class PipelineLibraryFile(BaseModel):
    """
    Pipeline Library File specifications

    Attributes
    ----------
    path:
        Workspace filepath
    """

    path: str


class PipelineLibraryNotebook(BaseModel):
    """
    Pipeline Library Notebook specifications

    Attributes
    ----------
    path:
        Workspace notebook filepath
    """

    path: str


class PipelineLibrary(BaseModel):
    """
    Pipeline Library specifications

    Attributes
    ----------
    file:
        File specifications
    notebook:
        Notebook specifications
    """

    file: str = None
    notebook: PipelineLibraryNotebook = None


class PipelineNotifications(BaseModel):
    """
    Pipeline Notifications specifications

    Attributes
    ----------
    alerts:
        Alert types
    recipients:
        List of user/group/service principal names
    """

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
    """
    Pipeline Cluster. Same attributes as `laktory.models.Cluster`, except for

    * `autotermination_minutes`
    * `cluster_id`
    * `data_security_mode`
    * `enable_elastic_disk`
    * `idempotency_token`
    * `is_pinned`
    * `libraries`
    * `node_type_id`
    * `runtime_engine`
    * `single_user_name`
    * `spark_version`

    that are not allowed.
    """

    autotermination_minutes: int = Field(None, exclude=True)
    cluster_id: str = Field(None, exclude=True)
    data_security_mode: str = Field(None, exclude=True)
    enable_elastic_disk: bool = Field(None, exclude=True)
    idempotency_token: str = Field(None, exclude=True)
    is_pinned: bool = Field(None, exclude=True)
    libraries: list[Any] = Field(None, exclude=True)
    node_type_id: str = None
    runtime_engine: str = Field(None, exclude=True)
    single_user_name: str = Field(None, exclude=True)
    spark_version: str = Field(None, exclude=True)

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
    """
    Pipeline User Define Function

    Attributes
    ----------
    module_name:
        Name of the module from which the function needs to be imported.
    function_name:
        Name of the function.
    module_path:
        Workspace filepath of the module, if not in the same directory as the pipeline notebook
    """

    module_name: str
    function_name: str
    module_path: str = None


class Pipeline(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Delta Live Tables (DLT) Pipeline

    Attributes
    ----------
    access_controls:
        Pipeline access controls
    allow_duplicate_names:
        If `False`, deployment will fail if name conflicts with that of another pipeline.
    catalog:
        Name of the unity catalog storing the pipeline tables
    channel:
        Name of the release channel for Spark version used by DLT pipeline.
    clusters:
        Clusters to run the pipeline. If none is specified, pipelines will automatically select a default cluster
        configuration for the pipeline.
    configuration:
         List of values to apply to the entire pipeline. Elements must be formatted as key:value pairs
    continuous:
        If `True`, the pipeline is run continuously.
    development:
        If `True` the pipeline is run in development mode
    edition:
        Name of the product edition
    libraries:
        Specifies pipeline code (notebooks) and required artifacts.
    name:
        Pipeline name
    notifications:
        Notifications specifications
    photon:
        If `True`, Photon engine enabled.
    serverless:
        If `True`, serverless is enabled
    storage:
        A location on DBFS or cloud storage where output data and metadata required for pipeline execution are stored.
        By default, tables are stored in a subdirectory of this location. Change of this parameter forces recreation
        of the pipeline. (Conflicts with `catalog`).
    tables:
        List of tables to build
    target:
        The name of a database (in either the Hive metastore or in a UC catalog) for persisting pipeline output data.
        Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI.
    udfs:
        List of user defined functions provided to the table builders.

    Examples
    --------
    Assuming the configuration yaml file
    ```py
    import io
    from laktory import models

    # Define pipeline
    pipeline_yaml = '''
    name: pl-stock-prices

    catalog: dev
    target: finance

    clusters:
      - name : default
        node_type_id: Standard_DS3_v2
        autoscale:
          min_workers: 1
          max_workers: 2

    libraries:
      - notebook:
          path: /pipelines/dlt_brz_template.py
      - notebook:
          path: /pipelines/dlt_slv_template.py
      - notebook:
          path: /pipelines/dlt_gld_stock_performances.py

    access_controls:
      - group_name: account users
        permission_level: CAN_VIEW
      - group_name: role-engineers
        permission_level: CAN_RUN

    # --------------------------------------------------------------------------- #
    # Tables                                                                      #
    # --------------------------------------------------------------------------- #

    tables:
      - name: brz_stock_prices
        timestamp_key: data.created_at
        builder:
          layer: BRONZE
          event_source:
            name: stock_price
            producer:
              name: yahoo-finance
            read_as_stream: True

      - name: slv_stock_prices
        timestamp_key: created_at
        builder:
          layer: SILVER
          table_source:
            name: brz_stock_prices
            read_as_stream: True
        columns:
          - name: created_at
            type: timestamp
            spark_func_name: coalesce
            spark_func_args:
              - data._created_at

          - name: symbol
            type: string
            spark_func_name: coalesce
            spark_func_args:
              - data.symbol

          - name: open
            type: double
            spark_func_name: coalesce
            spark_func_args:
              - data.open

          - name: close
            type: double
            spark_func_name: coalesce
            spark_func_args:
              - data.close
    '''

    # Read pipeline
    pipeline = models.Pipeline.model_validate_yaml(io.StringIO(pipeline_yaml))

    # Deploy pipeline
    pipeline.to_pulumi()
    ```

    References
    ----------

    * [Databricks Pipeline](https://docs.databricks.com/api/workspace/pipelines/create)
    * [Pulumi Databricks Pipeline](https://www.pulumi.com/registry/packages/databricks/api-docs/pipeline/)
    """

    access_controls: list[AccessControl] = []
    allow_duplicate_names: bool = None
    catalog: str = None
    channel: Literal["CURRENT", "PREVIEW"] = "PREVIEW"
    clusters: list[PipelineCluster] = []
    configuration: dict[str, str] = {}
    continuous: bool = None
    development: Union[bool, str] = None
    edition: Literal["CORE", "PRO", "ADVANCED"] = None
    # filters
    libraries: list[PipelineLibrary] = []
    name: str
    notifications: list[PipelineNotifications] = []
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
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """
        pl
        """
        return "pl"

    @property
    def core_resources(self) -> list[PulumiResource]:
        """
        - pipeline
        - permissions
        - configuration workspace file
        - configuration workspace file permissions
        """
        if self._core_resources is None:
            self._core_resources = [
                self,
            ]
            if self.access_controls:
                self._core_resources += [
                    Permissions(
                        resource_name=f"permissions-{self.resource_name}",
                        access_controls=self.access_controls,
                        pipeline_id=f"${{resources.{self.resource_name}.id}}",
                    )
                ]

            # Configuration file
            source = os.path.join(CACHE_ROOT, f"tmp-{self.name}.json")
            d = self.model_dump(exclude_none=True)
            d = self.inject_vars(d)
            s = json.dumps(d, indent=4)
            with open(source, "w", newline="\n") as fp:
                fp.write(s)
            filepath = f"{settings.workspace_laktory_root}pipelines/{self.name}.json"
            file = WorkspaceFile(
                path=filepath,
                source=source,
            )
            self._core_resources += [file]

            self._core_resources += [
                Permissions(
                    resource_name=f"permissions-file-{file.resource_name}",
                    access_controls=[
                        AccessControl(
                            permission_level="CAN_READ",
                            group_name="account users",
                        )
                    ],
                    workspace_file_path=filepath,
                    options={"depends_on": [f"${{resources.{file.resource_name}}}"]},
                )
            ]

        return self._core_resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Pipeline"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Pipeline

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return {
            "access_controls": True,
            "tables": True,
            "clusters": {"__all__": {"access_controls"}},
            "udfs": True,
        }

    @property
    def pulumi_properties(self):
        d = super().pulumi_properties
        k = "clusters"
        if k in d:
            _clusters = []
            for c in d[k]:
                c["label"] = c.pop("name")
                _clusters += [c]
            d[k] = _clusters
        return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_pipeline"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties
        k = "cluster"
        if k in d:
            _clusters = []
            for c in d[k]:
                c["label"] = c.pop("name")
                _clusters += [c]
            d[k] = _clusters
        return d
