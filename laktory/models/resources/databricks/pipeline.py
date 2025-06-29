from typing import Any
from typing import Literal
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class PipelineTriggerCron(BaseModel):
    quartz_cron_schedule: str = Field(None, description="")
    timezone_id: str = Field(None, description="")


class PipelineTrigger(BaseModel):
    cron: PipelineTriggerCron = Field(None, description="")
    # manual:


class PipelineRunAs(BaseModel):
    service_principal_name: str = Field(None, description="")
    user_name: str = Field(None, description="")


class PipelineRestartWindow(BaseModel):
    start_hour: int = Field(..., description="")
    days_of_weeks: list[str] = Field(None, description="")
    time_zone_id: str = Field(None, description="")


class PipelineLatestUpdate(BaseModel):
    creation_time: str = Field(None, description="")
    state: str = Field(None, description="")
    update_id: str = Field(None, description="")


class PipelineGatewayDefinition(BaseModel):
    connection_id: str = Field(
        None,
        description="Immutable. The Unity Catalog connection this gateway pipeline uses to communicate with the source.",
    )
    connection_name: str = Field(..., description="")
    gateway_storage_catalog: str = Field(
        ...,
        description="Required, Immutable. The name of the catalog for the gateway pipeline's storage location.",
    )
    gateway_storage_name: str = Field(
        None,
        description="""
        Required. The Unity Catalog-compatible naming for the gateway storage location. This is the destination to use 
        for the data that is extracted by the gateway. Delta Live Tables system will automatically create the storage 
        location under the catalog and schema.
        """,
    )
    gateway_storage_schema: str = Field(
        ...,
        description="Required, Immutable. The name of the schema for the gateway pipelines's storage location.",
    )


class PipelineDeployment(BaseModel):
    kind: str = Field(
        ..., description="The deployment method that manages the pipeline."
    )
    metadata_file_path: str = Field(
        ...,
        description="The path to the file containing metadata about the deployment.",
    )


class PipelineEventLog(BaseModel):
    name: str = Field(
        ..., description="The table name the event log is published to in UC."
    )
    catalog: str = Field(
        ..., description="The UC catalog the event log is published under."
    )
    schema_: str = Field(
        ...,
        description="The UC schema the event log is published under.",
        validation_alias=AliasChoices("schema", "schema_"),
    )


class PipelineFilters(BaseModel):
    excludes: str = Field(..., description="Paths to exclude.")
    includes: str = Field(..., description="Paths to include.")


class PipelineLibraryFile(BaseModel):
    path: str = Field(..., description="")


class PipelineLibraryNotebook(BaseModel):
    path: str = Field(..., description="Workspace notebook filepath")


class PipelineLibrary(BaseModel):
    file: str = Field(None, description="File specifications")
    notebook: PipelineLibraryNotebook = Field(
        None, description="Notebook specifications"
    )


class PipelineNotifications(BaseModel):
    alerts: list[
        Literal[
            "on-update-success",
            "on-update-failure",
            "on-update-fatal-failure",
            "on-flow-failure",
        ]
    ] = Field(..., description="Alert types")
    recipients: list[str] = Field(
        ..., description="List of user/group/service principal names"
    )


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
    * `no_wait`
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
    no_wait: bool = Field(None, exclude=True)
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
            "no_wait",
            "runtime_engine",
            "single_user_name",
            "spark_version",
        ]:
            if getattr(self, f, None) not in [None, [], {}]:
                raise ValueError(f"Field {f} should be null")

        return self


class Pipeline(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Lakeflow Declarative Pipeline (formerly Delta Live Tables)

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

    '''
    pipeline = models.resources.databricks.Pipeline.model_validate_yaml(
        io.StringIO(pipeline_yaml)
    )
    ```

    References
    ----------

    * [Databricks Pipeline](https://docs.databricks.com/api/workspace/pipelines/create)
    * [Pulumi Databricks Pipeline](https://www.pulumi.com/registry/packages/databricks/api-docs/pipeline/)
    """

    access_controls: list[AccessControl] = Field(
        [], description="Pipeline access controls"
    )
    allow_duplicate_names: bool = Field(
        None,
        description="""
        If `False`, deployment will fail if name conflicts with that of another pipeline.
        """,
    )
    budget_policy_id: str = Field(
        None,
        description="optional string specifying ID of the budget policy for this DLT pipeline.",
    )
    catalog: Union[str, None] = Field(
        None, description="Name of the unity catalog storing the pipeline tables"
    )
    cause: str = Field(None, description="")
    channel: Literal["CURRENT", "PREVIEW"] = Field(
        "PREVIEW",
        description="Name of the release channel for Spark version used by DLT pipeline.",
    )
    cluster_id: str = Field(None, description="")
    clusters: list[PipelineCluster] = Field(
        [],
        description="""
        Clusters to run the pipeline. If none is specified, pipelines will automatically select a default cluster 
        configuration for the pipeline.
        """,
    )
    creator_user_name: str = Field(None, description="")
    configuration: dict[str, str] = Field(
        {},
        description="List of values to apply to the entire pipeline. Elements must be formatted as key:value pairs",
    )
    continuous: bool = Field(
        None, description="If `True`, the pipeline is run continuously."
    )
    deployment: PipelineDeployment = Field(
        None, description="Deployment type of this pipeline."
    )
    development: bool = Field(
        None, description="If `True` the pipeline is run in development mode"
    )
    edition: Literal["CORE", "PRO", "ADVANCED"] = Field(
        None, description="Name of the product edition"
    )
    event_log: PipelineEventLog = Field(
        None,
        description="An optional block specifying a table where DLT Event Log will be stored.",
    )
    expected_last_modified: int = Field(None, description="")
    filters: PipelineFilters = Field(
        None,
        description="Filters on which Pipeline packages to include in the deployed graph.",
    )
    gateway_definition: PipelineGatewayDefinition = Field(
        None, description="The definition of a gateway pipeline to support CDC."
    )
    health: str = Field(None, description="")
    # ingestion_definition: = Field(None, description="")  #TODO
    last_modified: int = Field(None, description="")
    latest_updates: list[PipelineLatestUpdate] = Field(None, description="")
    libraries: list[PipelineLibrary] = Field(
        None, description="Specifies pipeline code (notebooks) and required artifacts."
    )
    name: str = Field(..., description="Pipeline name")
    name_prefix: str = Field(None, description="Prefix added to the DLT pipeline name")
    name_suffix: str = Field(None, description="Suffix added to the DLT pipeline name")
    notifications: list[PipelineNotifications] = Field(
        [], description="Notifications specifications"
    )
    photon: bool = Field(None, description="If `True`, Photon engine enabled.")
    restart_window: PipelineRestartWindow = Field(None, description="")
    root_path: str = Field(
        None,
        description="""
        An optional string specifying the root path for this pipeline. This is used as the root directory when editing
        the pipeline in the Databricks user interface and it is added to sys.path when executing Python sources during 
        pipeline execution.
        """,
    )
    run_as: PipelineRunAs = Field(None, description="")
    run_as_user_name: str = Field(None, description="")
    schema_: str = Field(
        None,
        description="""
        The default schema (database) where tables are read from or published to. The presence of this
        attribute implies that the pipeline is in direct publishing mode.
        """,
        validation_alias=AliasChoices("schema", "schema_"),
    )
    serverless: bool = Field(None, description="If `True`, serverless is enabled")
    state: str = Field(None, description="")
    storage: str = Field(
        None,
        description="""
        A location on DBFS or cloud storage where output data and metadata required for pipeline execution are stored.
        By default, tables are stored in a subdirectory of this location. Change of this parameter forces recreation of
        the pipeline. (Conflicts with `catalog`).
        """,
    )
    target: str = Field(
        None,
        description="""
        The name of a database (in either the Hive metastore or in a UC catalog) for persisting pipeline output data.
        Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI.
        """,
    )
    trigger: PipelineTrigger = Field(None, description="")
    url: str = Field(
        None, description="URL of the DLT pipeline on the given workspace."
    )

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.name = self.name_prefix + self.name
                self.name_prefix = ""
            if self.name_suffix:
                self.name = self.name + self.name_suffix
                self.name_suffix = ""
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """
        dlt
        """
        return "dlt-pipeline"

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []

        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    pipeline_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Pipeline"

    @property
    def pulumi_excludes(self) -> list[str] | dict[str, bool]:
        return {
            "access_controls": True,
            "clusters": {"__all__": {"access_controls"}},
            "name_prefix": True,
            "name_suffix": True,
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
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
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
