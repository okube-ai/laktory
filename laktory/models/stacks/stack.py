from typing import Union
from typing import Any
from typing import Literal
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._parsers import merge_dicts
from laktory.models.basemodel import BaseModel
from laktory.models.databricks.cluster import Cluster
from laktory.models.databricks.directory import Directory
from laktory.models.databricks.group import Group
from laktory.models.databricks.job import Job
from laktory.models.databricks.notebook import Notebook
from laktory.models.databricks.pipeline import Pipeline
from laktory.models.databricks.secret import Secret
from laktory.models.databricks.secretscope import SecretScope
from laktory.models.databricks.serviceprincipal import ServicePrincipal
from laktory.models.databricks.sqlquery import SqlQuery
from laktory.models.databricks.user import User
from laktory.models.databricks.warehouse import Warehouse
from laktory.models.databricks.workspacefile import WorkspaceFile
from laktory.models.providers.baseprovider import BaseProvider
from laktory.models.providers.awsprovider import AWSProvider
from laktory.models.providers.azureprovider import AzureProvider
from laktory.models.providers.azurepulumiprovider import AzurePulumiProvider
from laktory.models.providers.databricksprovider import DatabricksProvider
from laktory.models.sql.catalog import Catalog
from laktory.models.sql.schema import Schema
from laktory.models.sql.table import Table
from laktory.models.sql.volume import Volume

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
    catalogs:
        Catalogs
    clusters:
        Clusters
    directories:
        Directories
    groups:
        Groups
    jobs:
        Jobs
    notebooks:
        Notebooks
    pipelines:
        Pipelines
    schemas:
        Schemas
    secretscopes:
        SecretScopes
    serviceprincipals:
        ServicePrincipals
    sqlqueries:
        SQLQueries
    tables:
        Tables
    providers:
        Providers
    users:
        Users
    volumes:
        Volumes
    warehouses:
        Warehouses
    workspacefiles:
        WorkspacFiles
    """

    catalogs: dict[str, Catalog] = {}
    clusters: dict[str, Cluster] = {}
    directories: dict[str, Directory] = {}
    groups: dict[str, Group] = {}
    jobs: dict[str, Job] = {}
    notebooks: dict[str, Notebook] = {}
    pipelines: dict[str, Pipeline] = {}
    schemas: dict[str, Schema] = {}
    secrets: dict[str, Secret] = {}
    secretscopes: dict[str, SecretScope] = {}
    serviceprincipals: dict[str, ServicePrincipal] = {}
    sqlqueries: dict[str, SqlQuery] = {}
    tables: dict[str, Table] = {}
    providers: dict[
        str, Union[AWSProvider, AzureProvider, AzurePulumiProvider, DatabricksProvider]
    ] = {}
    users: dict[str, User] = {}
    volumes: dict[str, Volume] = {}
    warehouses: dict[str, Warehouse] = {}
    workspacefiles: dict[str, WorkspaceFile] = {}

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
    resources: StackResources = StackResources()
    terraform: Terraform = Terraform()
    variables: dict[str, Union[str, bool]] = {}


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
    variables: dict[str, Union[str, bool]] = None


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
            "pipelines": {
                "pl-stock-prices": {
                    "name": "pl-stock-prices",
                    "development": "${vars.is_dev}",
                    "libraries": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}},
                    ],
                }
            },
            "jobs": {
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
                                "pipeline_id": "${resources.pl-stock-prices.id}",
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
    variables={'org': 'okube'} backend='pulumi' description=None environments={'dev': EnvironmentSettings(variables={'is_dev': True}, resources=None), 'prod': EnvironmentSettings(variables={'is_dev': False}, resources=None)} name='workspace' organization=None pulumi=Pulumi(variables={}, config={'databricks:host': '${vars.DATABRICKS_HOST}', 'databricks:token': '${vars.DATABRICKS_TOKEN}'}, outputs={}) resources=StackResources(variables={}, catalogs={}, clusters={}, directories={}, groups={}, jobs={'job-stock-prices': Job(resource_name_='job-stock-prices', options=ResourceOptions(variables={}, depends_on=[], provider=None, aliases=None, delete_before_replace=True, ignore_changes=None, import_=None, parent=None, replace_on_changes=None), variables={}, access_controls=[], clusters=[JobCluster(resource_name_=None, options=ResourceOptions(variables={}, depends_on=[], provider=None, aliases=None, delete_before_replace=True, ignore_changes=None, import_=None, parent=None, replace_on_changes=None), variables={}, access_controls=None, apply_policy_default_values=None, autoscale=None, autotermination_minutes=None, cluster_id=None, custom_tags=None, data_security_mode='USER_ISOLATION', driver_instance_pool_id=None, driver_node_type_id=None, enable_elastic_disk=None, enable_local_disk_encryption=None, idempotency_token=None, init_scripts=[], instance_pool_id=None, is_pinned=None, libraries=None, name='main', node_type_id='Standard_DS3_v2', num_workers=None, policy_id=None, runtime_engine=None, single_user_name=None, spark_conf={}, spark_env_vars={}, spark_version='14.0.x-scala2.12', ssh_public_keys=[])], continuous=None, control_run_state=None, email_notifications=None, format=None, health=None, max_concurrent_runs=None, max_retries=None, min_retry_interval_millis=None, name='job-stock-prices', notification_settings=None, parameters=[], retry_on_timeout=None, run_as=None, schedule=None, tags={}, tasks=[JobTask(variables={}, condition_task=None, depends_ons=None, description=None, email_notifications=None, existing_cluster_id=None, health=None, job_cluster_key='main', libraries=None, max_retries=None, min_retry_interval_millis=None, notebook_task=JobTaskNotebookTask(variables={}, notebook_path='/.laktory/jobs/ingest_stock_prices.py', base_parameters=None, source=None), notification_settings=None, pipeline_task=None, retry_on_timeout=None, run_if=None, run_job_task=None, sql_task=None, task_key='ingest', timeout_seconds=None), JobTask(variables={}, condition_task=None, depends_ons=[JobTaskDependsOn(variables={}, task_key='ingest', outcome=None)], description=None, email_notifications=None, existing_cluster_id=None, health=None, job_cluster_key=None, libraries=None, max_retries=None, min_retry_interval_millis=None, notebook_task=None, notification_settings=None, pipeline_task=JobTaskPipelineTask(variables={}, pipeline_id='${resources.pl-stock-prices.id}', full_refresh=None), retry_on_timeout=None, run_if=None, run_job_task=None, sql_task=None, task_key='pipeline', timeout_seconds=None)], timeout_seconds=None, trigger=None, webhook_notifications=None)}, notebooks={}, pipelines={'pl-stock-prices': Pipeline(resource_name_='pl-stock-prices', options=ResourceOptions(variables={}, depends_on=[], provider=None, aliases=None, delete_before_replace=True, ignore_changes=None, import_=None, parent=None, replace_on_changes=None), variables={}, access_controls=[], allow_duplicate_names=None, catalog=None, channel='PREVIEW', clusters=[], configuration={}, continuous=None, development='${vars.is_dev}', edition=None, libraries=[PipelineLibrary(variables={}, file=None, notebook=PipelineLibraryNotebook(variables={}, path='/pipelines/dlt_brz_template.py'))], name='pl-stock-prices', notifications=[], photon=None, serverless=None, storage=None, tables=[], target=None, udfs=[])}, schemas={}, secrets={}, secretscopes={}, serviceprincipals={}, sqlqueries={}, tables={}, providers={}, users={}, volumes={}, warehouses={}, workspacefiles={}) terraform=Terraform(variables={}, backend=None)
    '''
    ```

    """

    backend: Literal["pulumi", "terraform"] = None
    description: str = None
    environments: dict[str, EnvironmentSettings] = {}
    name: str
    organization: str = None
    pulumi: Pulumi = Pulumi()
    resources: StackResources = StackResources()
    terraform: Terraform = Terraform()
    variables: dict[str, Union[str, bool]] = {}

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def envs(self) -> dict[str, EnvironmentStack]:
        """
        Complete definition of each of the stack environments. It takes into
        account both the default stack values and environment-specific
        overwrites.

        Returns
        -------
        :
            Environment definitions.
        """
        ENV_FIELDS = ["pulumi", "resources", "terraform", "variables"]

        d = self.model_dump(exclude_none=True)
        _envs = d.pop("environments")

        # Restore fields excluded from dump
        for rtype in d["resources"]:
            for k, r in d["resources"][rtype].items():
                r["options"] = getattr(self.resources, rtype)[k].options.model_dump(
                    exclude_none=True
                )

        envs = {}
        for env_name, env in self.environments.items():
            for k in ENV_FIELDS:
                d[k] = merge_dicts(d[k], _envs[env_name].get(k, {}))

            envs[env_name] = EnvironmentStack(**d)

        return envs

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    def to_pulumi(self, env: Union[str, None] = None):
        """
        Create a pulumi stack for a given environment `env`.

        Parameters
        ----------
        env:
            Target environment. If `None`, used default stack values only.

        Returns
        -------
        : PulumiStack
            Pulumi-specific stack definition
        """
        from laktory.models.stacks.pulumistack import PulumiStack

        if env is not None and env in self.envs:
            env = self.envs[env]
        else:
            env = self

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
            variables=env.variables,
            outputs=env.pulumi.outputs,
        )

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def to_terraform(self, env: Union[str, None] = None):
        """
        Create a terraform stack for a given environment `env`.

        Parameters
        ----------
        env:
            Target environment. If `None`, used default stack values only.

        Returns
        -------
        : TerraformStack
            Terraform-specific stack definition
        """
        from laktory.models.stacks.terraformstack import TerraformStack

        if env is not None and env in self.envs:
            env = self.envs[env]
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
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        # Update terraform
        return TerraformStack(
            terraform={"backend": env.terraform.backend},
            providers=providers,
            resources=resources,
            variables=env.variables,
        )
