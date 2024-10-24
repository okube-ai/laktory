import copy
from typing import Union
from typing import Any
from typing import Literal
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._parsers import merge_dicts
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline import Pipeline
from laktory.models.resources.baseresource import ResourceOptions
from laktory.models.resources.databricks.catalog import Catalog
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.dashboard import Dashboard
from laktory.models.resources.databricks.dbfsfile import DbfsFile
from laktory.models.resources.databricks.directory import Directory
from laktory.models.resources.databricks.dltpipeline import DLTPipeline
from laktory.models.resources.databricks.externallocation import ExternalLocation
from laktory.models.resources.databricks.group import Group
from laktory.models.resources.databricks.grants import Grants
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.metastore import Metastore
from laktory.models.resources.databricks.metastoredataaccess import MetastoreDataAccess
from laktory.models.resources.databricks.mwsnetworkconnectivityconfig import (
    MwsNetworkConnectivityConfig,
)
from laktory.models.resources.databricks.notebook import Notebook
from laktory.models.resources.databricks.schema import Schema
from laktory.models.resources.databricks.secret import Secret
from laktory.models.resources.databricks.secretscope import SecretScope
from laktory.models.resources.databricks.serviceprincipal import ServicePrincipal
from laktory.models.resources.databricks.sqlquery import SqlQuery
from laktory.models.resources.databricks.table import Table
from laktory.models.resources.databricks.user import User
from laktory.models.resources.databricks.vectorsearchendpoint import (
    VectorSearchEndpoint,
)
from laktory.models.resources.databricks.vectorsearchindex import VectorSearchIndex
from laktory.models.resources.databricks.volume import Volume
from laktory.models.resources.databricks.warehouse import Warehouse
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.providers.awsprovider import AWSProvider
from laktory.models.resources.providers.azureprovider import AzureProvider
from laktory.models.resources.providers.azurepulumiprovider import AzurePulumiProvider
from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.providers.databricksprovider import DatabricksProvider

logger = get_logger(__name__)

DIRPATH = "./"


class Terraform(BaseModel):
    backend: dict[str, Any] = None


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
    databricks_dbfsfiles:
        Databricks DbfsFiles
    databricks_catalogs:
        Databricks Catalogs
    databricks_clusters:
        Databricks Clusters
    databricks_dashboards:
        Databricks Dashboards
    databricks_directories:
        Databricks Directories
    databricks_externallocations:
        Databricks External Locations
    databricks_groups:
        Databricks Groups
    databricks_grants:
        Databricks Grants
    databricks_jobs:
        Databricks Jobs
    databricks_metastores:
        Databricks Metastores
    databricks_networkconnectivityconfig
        Databricks Network Connectivity Config
    databricks_notebooks:
        Databricks Notebooks
    databricks_dltpipelines:
        Databricks DLT Pipelines
    databricks_schemas:
        Databricks Schemas
    databricks_secretscopes:
        Databricks SecretScopes
    databricks_serviceprincipals:
        Databricks ServicePrincipals
    databricks_sqlqueries:
        Databricks SQLQueries
    databricks_tables:
        Databricks Tables
    providers:
        Providers
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
    databricks_workspacefiles:
        Databricks WorkspacFiles
    pipelines:
        Laktory Pipelines
    """

    databricks_dashboards: dict[str, Dashboard] = {}
    databricks_dbfsfiles: dict[str, DbfsFile] = {}
    databricks_catalogs: dict[str, Catalog] = {}
    databricks_clusters: dict[str, Cluster] = {}
    databricks_directories: dict[str, Directory] = {}
    databricks_externallocations: dict[str, ExternalLocation] = {}
    databricks_grants: dict[str, Grants] = {}
    databricks_groups: dict[str, Group] = {}
    databricks_jobs: dict[str, Job] = {}
    databricks_metastoredataaccesses: dict[str, MetastoreDataAccess] = {}
    databricks_metastores: dict[str, Metastore] = {}
    databricks_networkconnectivityconfig: dict[str, MwsNetworkConnectivityConfig] = {}
    databricks_notebooks: dict[str, Notebook] = {}
    databricks_dltpipelines: dict[str, DLTPipeline] = {}
    databricks_schemas: dict[str, Schema] = {}
    databricks_secrets: dict[str, Secret] = {}
    databricks_secretscopes: dict[str, SecretScope] = {}
    databricks_serviceprincipals: dict[str, ServicePrincipal] = {}
    databricks_sqlqueries: dict[str, SqlQuery] = {}
    databricks_tables: dict[str, Table] = {}
    databricks_users: dict[str, User] = {}
    databricks_volumes: dict[str, Volume] = {}
    databricks_vectorsearchendpoints: dict[str, VectorSearchEndpoint] = {}
    databricks_vectorsearchindexes: dict[str, VectorSearchIndex] = {}
    databricks_warehouses: dict[str, Warehouse] = {}
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
    """

    resources: Any = None
    variables: dict[str, Any] = None


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

    print(stack)
    '''
    variables={'org': 'okube'} backend='pulumi' description=None environments={'dev': EnvironmentSettings(variables={'is_dev': True}, resources=None), 'prod': EnvironmentSettings(variables={'is_dev': False}, resources=None)} name='workspace' organization=None pulumi=Pulumi(variables={}, config={'databricks:host': '${vars.DATABRICKS_HOST}', 'databricks:token': '${vars.DATABRICKS_TOKEN}'}, outputs={}) resources=StackResources(variables={}, databricks_dashboards={}, databricks_dbfsfiles={}, databricks_catalogs={}, databricks_clusters={}, databricks_directories={}, databricks_externallocations={}, databricks_grants={}, databricks_groups={}, databricks_jobs={'job-stock-prices': Job(resource_name_='job-stock-prices', options=ResourceOptions(variables={}, depends_on=[], provider=None, ignore_changes=None, aliases=None, delete_before_replace=True, import_=None, parent=None, replace_on_changes=None), lookup_existing=None, variables={}, access_controls=[], clusters=[JobCluster(resource_name_=None, options=ResourceOptions(variables={}, depends_on=[], provider=None, ignore_changes=None, aliases=None, delete_before_replace=True, import_=None, parent=None, replace_on_changes=None), lookup_existing=None, variables={}, access_controls=None, apply_policy_default_values=None, autoscale=None, autotermination_minutes=None, cluster_id=None, custom_tags=None, data_security_mode='USER_ISOLATION', driver_instance_pool_id=None, driver_node_type_id=None, enable_elastic_disk=None, enable_local_disk_encryption=None, idempotency_token=None, init_scripts=[], instance_pool_id=None, is_pinned=None, libraries=None, name='main', node_type_id='Standard_DS3_v2', no_wait=None, num_workers=None, policy_id=None, runtime_engine=None, single_user_name=None, spark_conf={}, spark_env_vars={}, spark_version='14.0.x-scala2.12', ssh_public_keys=[])], continuous=None, control_run_state=None, description=None, email_notifications=None, format=None, health=None, max_concurrent_runs=None, max_retries=None, min_retry_interval_millis=None, name='job-stock-prices', notification_settings=None, parameters=[], retry_on_timeout=None, run_as=None, schedule=None, tags={}, tasks=[JobTask(variables={}, condition_task=None, depends_ons=None, description=None, email_notifications=None, existing_cluster_id=None, health=None, job_cluster_key='main', libraries=None, max_retries=None, min_retry_interval_millis=None, notebook_task=JobTaskNotebookTask(variables={}, notebook_path='/.laktory/jobs/ingest_stock_prices.py', base_parameters=None, warehouse_id=None, source=None), notification_settings=None, pipeline_task=None, retry_on_timeout=None, run_if=None, run_job_task=None, sql_task=None, task_key='ingest', timeout_seconds=None), JobTask(variables={}, condition_task=None, depends_ons=[JobTaskDependsOn(variables={}, task_key='ingest', outcome=None)], description=None, email_notifications=None, existing_cluster_id=None, health=None, job_cluster_key=None, libraries=None, max_retries=None, min_retry_interval_millis=None, notebook_task=None, notification_settings=None, pipeline_task=JobTaskPipelineTask(variables={}, pipeline_id='${resources.dlt-pl-stock-prices.id}', full_refresh=None), retry_on_timeout=None, run_if=None, run_job_task=None, sql_task=None, task_key='pipeline', timeout_seconds=None)], timeout_seconds=None, trigger=None, webhook_notifications=None)}, databricks_metastoredataaccesses={}, databricks_metastores={}, databricks_networkconnectivityconfig={}, databricks_notebooks={}, databricks_dltpipelines={'pl-stock-prices': DLTPipeline(resource_name_='pl-stock-prices', options=ResourceOptions(variables={}, depends_on=[], provider=None, ignore_changes=None, aliases=None, delete_before_replace=True, import_=None, parent=None, replace_on_changes=None), lookup_existing=None, variables={}, access_controls=[], allow_duplicate_names=None, catalog=None, channel='PREVIEW', clusters=[], configuration={}, continuous=None, development='${vars.is_dev}', edition=None, libraries=[PipelineLibrary(variables={}, file=None, notebook=PipelineLibraryNotebook(variables={}, path='/pipelines/dlt_brz_template.py'))], name='pl-stock-prices', notifications=[], photon=None, serverless=None, storage=None, target=None)}, databricks_schemas={}, databricks_secrets={}, databricks_secretscopes={}, databricks_serviceprincipals={}, databricks_sqlqueries={}, databricks_tables={}, databricks_users={}, databricks_volumes={}, databricks_vectorsearchendpoints={}, databricks_vectorsearchindexes={}, databricks_warehouses={}, databricks_workspacefiles={}, pipelines={}, providers={}) terraform=Terraform(variables={}, backend=None)
    '''
    ```

    """

    backend: Literal["pulumi", "terraform"] = None
    description: str = None
    environments: dict[str, EnvironmentSettings] = {}
    name: str
    organization: Union[str, None] = None
    pulumi: Pulumi = Pulumi()
    resources: Union[StackResources, None] = StackResources()
    terraform: Terraform = Terraform()
    variables: dict[str, Any] = {}
    _envs: dict[str, EnvironmentStack] = None

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def get_env(self, env_name: str, inject_vars=True) -> EnvironmentStack:
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

        if self._envs is None:

            ENV_FIELDS = ["pulumi", "resources", "terraform", "variables"]

            # Because options is an excluded field for all resources and
            # sub-resources, we need to manually dump it and add it to
            # the base dump
            def dump_with_options(obj: Any) -> Any:

                # Check data type, call recursively if not a BaseModel
                if isinstance(obj, list):
                    return [dump_with_options(v) for v in obj]
                elif isinstance(obj, dict):
                    return {k: dump_with_options(v) for k, v in obj.items()}
                elif not isinstance(obj, BaseModel):
                    return obj

                # Get model dump
                model = obj
                data = model.model_dump(exclude_unset=True)

                # Loop through all model fields
                for field_name, field in model.model_fields.items():

                    # Explicitly dump options if found in the model
                    if field_name == "options" and field.annotation == ResourceOptions:
                        data["options"] = model.options.model_dump(exclude_unset=True)

                    if field_name == "resource_name_" and model.resource_name_:
                        data["resource_name_"] = model.resource_name_

                    if field_name == "lookup_existing" and model.lookup_existing:
                        data["lookup_existing"] = model.lookup_existing.model_dump(
                            exclude_unset=True
                        )

                    # Parse list
                    if isinstance(data.get(field_name, None), list):
                        data[field_name] = [
                            dump_with_options(v) for v in getattr(model, field_name)
                        ]

                    # Parse dict (might result from a dict or a BaseModel)
                    elif isinstance(data.get(field_name, None), dict):
                        a = getattr(model, field_name)

                        if isinstance(a, dict):
                            for k in a.keys():
                                data[field_name][k] = dump_with_options(a[k])
                        else:
                            data[field_name] = dump_with_options(a)

                return data

            envs = {}
            for _env_name, env in self.environments.items():

                d = dump_with_options(self)
                _envs = d.pop("environments")

                for k in ENV_FIELDS:
                    v1 = _envs[_env_name].get(k, {})
                    if k in d:
                        d[k] = merge_dicts(d[k], v1)
                    elif k in _envs[_env_name]:
                        d[k] = v1

                # Inject Variables
                if inject_vars:
                    d = self.environments[_env_name].inject_vars(d)

                envs[_env_name] = EnvironmentStack(**d)

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

        if env_name is not None:
            if env_name in self.environments.keys():
                env = self.get_env(env_name=env_name, inject_vars=False)
            else:
                raise ValueError(
                    f"Environment '{env_name}' is not declared in the stack."
                )
        else:
            env = self

        # Resources
        resources = {}
        for r in env.resources._get_all().values():
            r.variables = env.variables
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        return PulumiStack(
            name=env.name,
            organization=env.organization,
            config=env.pulumi.config,
            description=env.description,
            resources=resources,
            variables=env.variables,
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

        if env_name is not None:
            if env_name in self.environments.keys():
                env = self.get_env(env_name=env_name, inject_vars=False)
            else:
                raise ValueError(
                    f"Environment '{env_name}' is not declared in the stack."
                )
        else:
            env = self

        # Providers
        providers = {}
        for r in env.resources._get_all(providers_only=True).values():
            for _r in r.core_resources:
                rname = _r.resource_name
                providers[rname] = _r

        # Resources
        resources = {}
        for r in env.resources._get_all(providers_excluded=True).values():
            r.variables = env.variables
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        # Update terraform
        return TerraformStack(
            terraform={"backend": env.terraform.backend},
            providers=providers,
            resources=resources,
            variables=env.variables,
        )
