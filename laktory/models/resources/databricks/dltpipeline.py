from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


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


class DLTPipeline(BaseModel, PulumiResource, TerraformResource):
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
    target:
        The name of a database (in either the Hive metastore or in a UC catalog) for persisting pipeline output data.
        Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI.

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
    pipeline = models.resources.databricks.DLTPipeline.model_validate_yaml(
        io.StringIO(pipeline_yaml)
    )
    ```

    References
    ----------

    * [Databricks Pipeline](https://docs.databricks.com/api/workspace/pipelines/create)
    * [Pulumi Databricks Pipeline](https://www.pulumi.com/registry/packages/databricks/api-docs/pipeline/)
    """

    access_controls: list[AccessControl] = []
    allow_duplicate_names: bool = None
    catalog: Union[str, None] = None
    channel: Literal["CURRENT", "PREVIEW"] = "PREVIEW"
    clusters: list[PipelineCluster] = []
    configuration: dict[str, str] = {}
    continuous: bool = None
    development: Union[bool, str] = None
    edition: Literal["CORE", "PRO", "ADVANCED"] = None
    # filters
    libraries: list[PipelineLibrary] = None
    name: str
    notifications: list[PipelineNotifications] = []
    photon: bool = None
    serverless: bool = None
    storage: str = None
    target: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """
        dlt
        """
        return "dlt"

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
    def pulumi_resource_type(self) -> str:
        return "databricks:Pipeline"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return {
            "access_controls": True,
            "clusters": {"__all__": {"access_controls"}},
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
