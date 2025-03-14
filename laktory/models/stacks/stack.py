import copy
from typing import Any
from typing import Literal
from typing import Union

from pydantic import model_validator

from laktory._logger import get_logger
from laktory._parsers import merge_dicts
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline.pipeline import Pipeline
from laktory.models.resources.baseresource import ResourceOptions
from laktory.models.resources.databricks.alert import Alert
from laktory.models.resources.databricks.catalog import Catalog
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.clusterpolicy import ClusterPolicy
from laktory.models.resources.databricks.dashboard import Dashboard
from laktory.models.resources.databricks.dbfsfile import DbfsFile
from laktory.models.resources.databricks.directory import Directory
from laktory.models.resources.databricks.dltpipeline import DLTPipeline
from laktory.models.resources.databricks.externallocation import ExternalLocation
from laktory.models.resources.databricks.grant import Grant
from laktory.models.resources.databricks.grants import Grants
from laktory.models.resources.databricks.group import Group
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.metastore import Metastore
from laktory.models.resources.databricks.metastoreassignment import MetastoreAssignment
from laktory.models.resources.databricks.metastoredataaccess import MetastoreDataAccess
from laktory.models.resources.databricks.mlflowexperiment import MLflowExperiment
from laktory.models.resources.databricks.mlflowmodel import MLflowModel
from laktory.models.resources.databricks.mlflowwebhook import MLflowWebhook
from laktory.models.resources.databricks.mwsnetworkconnectivityconfig import (
    MwsNetworkConnectivityConfig,
)
from laktory.models.resources.databricks.notebook import Notebook
from laktory.models.resources.databricks.query import Query
from laktory.models.resources.databricks.repo import Repo
from laktory.models.resources.databricks.schema import Schema
from laktory.models.resources.databricks.secret import Secret
from laktory.models.resources.databricks.secretscope import SecretScope
from laktory.models.resources.databricks.serviceprincipal import ServicePrincipal
from laktory.models.resources.databricks.table import Table
from laktory.models.resources.databricks.user import User
from laktory.models.resources.databricks.vectorsearchendpoint import (
    VectorSearchEndpoint,
)
from laktory.models.resources.databricks.vectorsearchindex import VectorSearchIndex
from laktory.models.resources.databricks.volume import Volume
from laktory.models.resources.databricks.warehouse import Warehouse
from laktory.models.resources.databricks.workspacebinding import WorkspaceBinding
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.providers.awsprovider import AWSProvider
from laktory.models.resources.providers.azureprovider import AzureProvider
from laktory.models.resources.providers.azurepulumiprovider import AzurePulumiProvider
from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.providers.databricksprovider import DatabricksProvider

logger = get_logger(__name__)

DIRPATH = "./"


class Terraform(BaseModel):
    backend: Union[dict[str, Any], None] = None


class LaktorySettings(BaseModel):
    """
    Laktory Settings

    Attributes
    ----------
    dataframe_backend:
        DataFrame backend
    laktory_root:
        Laktory cache root directory. Used when a pipeline needs to write
        checkpoint files.
    workspace_laktory_root:
        Root directory of a Databricks Workspace (excluding `"/Workspace") to
        which databricks objects like notebooks and workspace files are
        deployed.


    """

    dataframe_backend: str = None
    workspace_laktory_root: str = "/.laktory/"
    laktory_root: str = "/laktory/"

    @model_validator(mode="after")
    def apply_settings(self) -> Any:
        if self.dataframe_backend:
            settings.dataframe_backend = self.dataframe_backend

        if self.workspace_laktory_root:
            settings.workspace_laktory_root = self.workspace_laktory_root

        if self.laktory_root:
            settings.laktory_root = self.laktory_root

        return self


class Pulumi(BaseModel):
    """
    config:
        Pulumi configuration settings. Generally used to
        configure providers. See references for more details.
    outputs:
        Requested resources-related outputs. See references for details.

    References
    ----------
    - Pulumi [configuration](https://www.pulumi.com/docs/concepts/config/)
    - Pulumi [outputs](https://www.pulumi.com/docs/concepts/inputs-outputs/#outputs)
    """

    config: dict[str, str] = {}
    outputs: dict[str, str] = {}


class StackResources(BaseModel):
    """
    Resources definition for a given stack or stack environment.

    Attributes
    ----------
    databricks_alerts:
        Databricks Alerts
    databricks_dbfsfiles:
        Databricks DbfsFiles
    databricks_catalogs:
        Databricks Catalogs
    databricks_clusters:
        Databricks Clusters
    databricks_clusterpolicies:
        Databricks Cluster Policies
    databricks_dashboards:
        Databricks Dashboards
    databricks_directories:
        Databricks Directories
    databricks_dltpipelines:
        Databricks DLT Pipelines
    databricks_externallocations:
        Databricks External Locations
    databricks_groups:
        Databricks Groups
    databricks_grant:
        Databricks Grant
    databricks_grants:
        Databricks Grants
    databricks_jobs:
        Databricks Jobs
    databricks_metastoreassignments:
        Databricks Metastore Assignments
    databricks_metastoredataaccesses:
        Databricks Metastore Data Accesses
    databricks_metastores:
        Databricks Metastores
    databricks_mlflowexperiments:
        Databricks MLflow Experiments
    databricks_mlflowmodels:
        Databricks MLflow models
    databricks_mlflowwebhooks:
        Databricks MLflow webhooks
    databricks_networkconnectivityconfig
        Databricks Network Connectivity Config
    databricks_notebooks:
        Databricks Notebooks
    databricks_queries:
        Databricks Queries
    databricks_repo:
        Databricks Repo
    databricks_schemas:
        Databricks Schemas
    databricks_secretscopes:
        Databricks SecretScopes
    databricks_serviceprincipals:
        Databricks ServicePrincipals
    databricks_tables:
        Databricks Tables
    databricks_users:
        Databricks Users
    databricks_vectorsearchendpoint:
        Databricks Vector Search Endpoint
    databricks_vectorsearchindex:
        Databricks Vector Search Index
    databricks_volumes:
        Databricks Volumes
    databricks_warehouses:
        Databricks Warehouses
    databricks_workspacebindings:
        Databricks Workspace Bindings
    databricks_workspacefiles:
        Databricks WorkspacFiles
    pipelines:
        Laktory Pipelines
    providers:
        Providers
    """

    databricks_alerts: dict[str, Alert] = {}
    databricks_catalogs: dict[str, Catalog] = {}
    databricks_clusterpolicies: dict[str, ClusterPolicy] = {}
    databricks_clusters: dict[str, Cluster] = {}
    databricks_dashboards: dict[str, Dashboard] = {}
    databricks_dbfsfiles: dict[str, DbfsFile] = {}
    databricks_directories: dict[str, Directory] = {}
    databricks_dltpipelines: dict[str, DLTPipeline] = {}
    databricks_externallocations: dict[str, ExternalLocation] = {}
    databricks_grant: dict[str, Grant] = {}
    databricks_grants: dict[str, Grants] = {}
    databricks_groups: dict[str, Group] = {}
    databricks_jobs: dict[str, Job] = {}
    databricks_metastoreassignments: dict[str, MetastoreAssignment] = {}
    databricks_metastoredataaccesses: dict[str, MetastoreDataAccess] = {}
    databricks_metastores: dict[str, Metastore] = {}
    databricks_mlflowexperiments: dict[str, MLflowExperiment] = {}
    databricks_mlflowmodels: dict[str, MLflowModel] = {}
    databricks_mlflowwebhooks: dict[str, MLflowWebhook] = {}
    databricks_networkconnectivityconfig: dict[str, MwsNetworkConnectivityConfig] = {}
    databricks_notebooks: dict[str, Notebook] = {}
    databricks_queries: dict[str, Query] = {}
    databricks_repos: dict[str, Repo] = {}
    databricks_schemas: dict[str, Schema] = {}
    databricks_secrets: dict[str, Secret] = {}
    databricks_secretscopes: dict[str, SecretScope] = {}
    databricks_serviceprincipals: dict[str, ServicePrincipal] = {}
    databricks_tables: dict[str, Table] = {}
    databricks_users: dict[str, User] = {}
    databricks_vectorsearchendpoints: dict[str, VectorSearchEndpoint] = {}
    databricks_vectorsearchindexes: dict[str, VectorSearchIndex] = {}
    databricks_volumes: dict[str, Volume] = {}
    databricks_warehouses: dict[str, Warehouse] = {}
    databricks_workspacebindings: dict[str, WorkspaceBinding] = {}
    databricks_workspacefiles: dict[str, WorkspaceFile] = {}
    pipelines: dict[str, Pipeline] = {}
    providers: dict[
        str, Union[AWSProvider, AzureProvider, AzurePulumiProvider, DatabricksProvider]
    ] = {}

    @model_validator(mode="after")
    def update_resource_names(self) -> Any:
        for k, r in self._get_all().items():
            if r.resource_name_ and k != r.resource_name_:
                raise ValueError(
                    f"Provided resource name {r.resource_name_} does not match provided key {k}"
                )
            r.resource_name_ = k
        return self

    def _get_all(self, providers_excluded=False, providers_only=False):
        resources = {}
        for resource_type in self.model_fields.keys():
            if resource_type in ["variables"]:
                continue

            for resource_name, _r in getattr(self, resource_type).items():
                if resource_name in resources.keys():
                    raise ValueError(
                        f"Stack resource names are not unique. '{resource_name}' is already used."
                    )

                if providers_excluded and isinstance(_r, BaseProvider):
                    continue

                if providers_only and not isinstance(_r, BaseProvider):
                    continue

                resources[resource_name] = _r

        return resources


class EnvironmentStack(BaseModel):
    """
    Environment-specific stack definition.

    Attributes
    ----------
    backend:
        IaC backend used for deployment.
    description:
        Description of the stack
    name:
        Name of the stack. If Pulumi is used as a backend, it should match
        the name of the Pulumi project.
    organization:
        Organization
    pulumi:
        Pulumi-specific settings
    resources:
        Dictionary of resources to be deployed. Each key should be a resource
        type and each value should be a dictionary of resources who's keys are
        the resource names and the values the resources definitions.
    settings:
        Laktory settings
    terraform:
        Terraform-specific settings
    variables:
        Dictionary of variables made available in the resources definition.
    """

    backend: Literal["pulumi", "terraform"] = None
    description: str = None
    name: str
    organization: str = None
    pulumi: Pulumi = Pulumi()
    resources: Union[StackResources, None] = StackResources()
    settings: LaktorySettings = None
    terraform: Terraform = Terraform()
    variables: dict[str, Any] = {}


class EnvironmentSettings(BaseModel):
    """
    Settings overwrite for a specific environments

    Attributes
    ----------
    resources:
        Dictionary of resources to be deployed. Each key should be a resource
        type and each value should be a dictionary of resources who's keys are
        the resource names and the values the resources definitions.
    variables:
        Dictionary of variables made available in the resources definition.
    terraform:
        Terraform-specific settings
    """

    resources: Any = None
    variables: dict[str, Any] = None
    terraform: Terraform = Terraform()


class Stack(BaseModel):
    """
    The Stack defines a collection of deployable resources, the deployment
    configuration, some variables and the environment-specific settings.

    Attributes
    ----------
    backend:
        IaC backend used for deployment.
    description:
        Description of the stack
    environments:
        Environment-specific overwrite of config, resources or variables
        arguments.
    name:
        Name of the stack. If Pulumi is used as a backend, it should match
        the name of the Pulumi project.
    organization:
        Organization
    pulumi:
        Pulumi-specific settings
    resources:
        Dictionary of resources to be deployed. Each key should be a resource
        type and each value should be a dictionary of resources who's keys are
        the resource names and the values the resources definitions.
    settings:
        Laktory settings
    terraform:
        Terraform-specific settings
    variables:
        Dictionary of variables made available in the resources definition.

    Examples
    --------
    ```py
    from laktory import models

    stack = models.Stack(
        name="workspace",
        backend="pulumi",
        pulumi={
            "config": {
                "databricks:host": "${vars.DATABRICKS_HOST}",
                "databricks:token": "${vars.DATABRICKS_TOKEN}",
            },
        },
        resources={
            "databricks_dltpipelines": {
                "pl-stock-prices": {
                    "name": "pl-stock-prices",
                    "development": "${vars.is_dev}",
                    "libraries": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}},
                    ],
                }
            },
            "databricks_jobs": {
                "job-stock-prices": {
                    "name": "job-stock-prices",
                    "clusters": [
                        {
                            "name": "main",
                            "spark_version": "14.0.x-scala2.12",
                            "node_type_id": "Standard_DS3_v2",
                        }
                    ],
                    "tasks": [
                        {
                            "task_key": "ingest",
                            "job_cluster_key": "main",
                            "notebook_task": {
                                "notebook_path": "/.laktory/jobs/ingest_stock_prices.py",
                            },
                        },
                        {
                            "task_key": "pipeline",
                            "depends_ons": [{"task_key": "ingest"}],
                            "pipeline_task": {
                                "pipeline_id": "${resources.dlt-pl-stock-prices.id}",
                            },
                        },
                    ],
                }
            },
        },
        variables={
            "org": "okube",
        },
        environments={
            "dev": {
                "variables": {
                    "is_dev": True,
                }
            },
            "prod": {
                "variables": {
                    "is_dev": False,
                }
            },
        },
    )
    ```

    """

    backend: Literal["pulumi", "terraform"] = None
    description: str = None
    environments: dict[str, EnvironmentSettings] = {}
    name: str
    organization: Union[str, None] = None
    pulumi: Pulumi = Pulumi()
    resources: Union[StackResources, None] = StackResources()
    settings: LaktorySettings = None
    terraform: Terraform = Terraform()
    variables: dict[str, Any] = {}
    _envs: dict[str, EnvironmentStack] = None

    @model_validator(mode="before")
    @classmethod
    def apply_settings(cls, data: Any) -> Any:
        """Required to apply settings before instantiating resources and setting default values"""
        settings = data.get("settings", None)
        if settings:
            if not isinstance(settings, dict):
                settings = settings.model_dump()
            LaktorySettings(**settings)

        return data

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def get_env(self, env_name: str) -> EnvironmentStack:
        """
        Complete definition the stack for a given environment. It takes into
        account both the default stack values and environment-specific
        overwrites.

        Parameters
        ----------
        env_name:
            Name of the environment

        Returns
        -------
        :
            Environment definitions.
        """

        if env_name is None:
            env = self
            env.push_vars()
            return env

        if env_name not in self.environments.keys():
            raise ValueError(f"Environment '{env_name}' is not declared in the stack.")

        if self._envs is None:
            ENV_FIELDS = ["pulumi", "resources", "terraform", "variables"]

            # Because some fields are excluded from the dump, they need to be
            # manually dumped and added back to the base dump
            def dump_with_excluded(obj: Any) -> Any:
                # Check data type, call recursively if not a BaseModel
                if isinstance(obj, list):
                    return [dump_with_excluded(v) for v in obj]
                elif isinstance(obj, dict):
                    return {k: dump_with_excluded(v) for k, v in obj.items()}
                elif not isinstance(obj, BaseModel):
                    return obj

                # Get model dump
                model = obj
                data = model.model_dump(exclude_unset=True)

                # Loop through all model fields
                for field_name, field in model.model_fields.items():
                    # Explicitly dump excluded fields - variables
                    if field_name == "variables" and model.variables is not None:
                        data["variables"] = copy.deepcopy(model.variables)

                    # Explicitly dump excluded fields - resource options
                    if field_name == "options" and field.annotation == ResourceOptions:
                        data["options"] = model.options.model_dump(exclude_unset=True)

                    # Explicitly dump excluded fields - resource name
                    if field_name == "resource_name_" and model.resource_name_:
                        data["resource_name_"] = model.resource_name_

                    # Explicitly dump excluded fields - lookup existing
                    if field_name == "lookup_existing" and model.lookup_existing:
                        data["lookup_existing"] = model.lookup_existing.model_dump(
                            exclude_unset=True
                        )

                    # Parse list
                    if isinstance(data.get(field_name, None), list):
                        data[field_name] = [
                            dump_with_excluded(v) for v in getattr(model, field_name)
                        ]

                    # Parse dict (might result from a dict or a BaseModel)
                    elif isinstance(data.get(field_name, None), dict):
                        a = getattr(model, field_name)

                        if isinstance(a, dict):
                            for k in a.keys():
                                data[field_name][k] = dump_with_excluded(a[k])
                        else:
                            data[field_name] = dump_with_excluded(a)

                return data

            envs = {}
            for _env_name, env in self.environments.items():
                d = dump_with_excluded(self)
                _envs = d.pop("environments")

                for k in ENV_FIELDS:
                    v1 = _envs[_env_name].get(k, {})
                    if k in d:
                        d[k] = merge_dicts(d[k], v1)
                    elif k in _envs[_env_name]:
                        d[k] = v1

                envs[_env_name] = EnvironmentStack(**d)
                envs[_env_name].push_vars()

            self._envs = envs

        return self._envs[env_name]

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    def to_pulumi(self, env_name: Union[str, None] = None):
        """
        Create a pulumi stack for a given environment `env`.

        Parameters
        ----------
        env_name:
            Target environment. If `None`, used default stack values only.

        Returns
        -------
        : PulumiStack
            Pulumi-specific stack definition
        """
        from laktory.models.stacks.pulumistack import PulumiStack

        env = self.get_env(env_name=env_name).inject_vars()

        # Resources
        resources = {}
        for r in env.resources._get_all().values():
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        return PulumiStack(
            name=env.name,
            organization=env.organization,
            config=env.pulumi.config,
            description=env.description,
            resources=resources,
            outputs=env.pulumi.outputs,
        )

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def to_terraform(self, env_name: Union[str, None] = None):
        """
        Create a terraform stack for a given environment `env`.

        Parameters
        ----------
        env_name:
            Target environment. If `None`, used default stack values only.

        Returns
        -------
        : TerraformStack
            Terraform-specific stack definition
        """
        from laktory.models.stacks.terraformstack import TerraformStack

        env = self.get_env(env_name=env_name).inject_vars()

        # Providers
        providers = {}
        for r in env.resources._get_all(providers_only=True).values():
            for _r in r.core_resources:
                rname = _r.resource_name
                providers[rname] = _r

        # Resources
        resources = {}
        for r in env.resources._get_all(providers_excluded=True).values():
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        # Update terraform
        return TerraformStack(
            terraform={"backend": env.terraform.backend},
            providers=providers,
            resources=resources,
        )
