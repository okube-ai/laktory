from typing import Union
from typing import Any
from typing import Literal
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._parsers import merge_dicts
from laktory.models.basemodel import BaseModel
from laktory.models.databricks.cluster import Cluster
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
from laktory.models.providers.databricksprovider import DatabricksProvider
from laktory.models.sql.catalog import Catalog
from laktory.models.sql.schema import Schema
from laktory.models.sql.table import Table
from laktory.models.sql.volume import Volume
from laktory.models.stacks.pulumistack import PulumiStack

logger = get_logger(__name__)

DIRPATH = "./"


class StackResources(BaseModel):
    catalogs: dict[str, Catalog] = {}
    clusters: dict[str, Cluster] = {}
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
    providers: dict[str, Union[DatabricksProvider]] = {}
    users: dict[str, User] = {}
    volumes: dict[str, Volume] = {}
    warehouses: dict[str, Warehouse] = {}
    workspacefiles: dict[str, WorkspaceFile] = {}

    @model_validator(mode="after")
    def update_resource_names(self) -> Any:
        for k, r in self._all.items():
            if r.resource_name_ and k != r.resource_name_:
                raise ValueError(
                    f"Provided resource name {r.resource_name_} does not match provided key {k}"
                )
            r.resource_name_ = k
        return self

    @property
    def _all(self):
        resources = {}
        for resource_type in self.model_fields.keys():
            if resource_type in ["variables"]:
                continue

            for resource_name, _r in getattr(self, resource_type).items():
                resources[resource_name] = _r

        return resources


class EnvironmentStack(BaseModel):
    config: dict[str, str] = {}
    description: str = None
    engine: Literal["pulumi", "terraform"] = None
    name: str
    pulumi_outputs: dict[str, str] = {}
    resources: StackResources = StackResources()
    variables: dict[str, Union[str, bool]] = {}


class EnvironmentSettings(BaseModel):
    config: dict[str, str] = None
    resources: Any = None
    variables: dict[str, Union[str, bool]] = None


class Stack(BaseModel):
    """
    The Stack defines a group of deployable resources.
    """

    config: dict[str, str] = {}
    description: str = None
    name: str
    engine: Literal["pulumi", "terraform"] = None
    pulumi_outputs: dict[str, str] = {}
    resources: StackResources = StackResources()
    variables: dict[str, Union[str, bool]] = {}
    environments: dict[str, EnvironmentSettings] = {}

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def envs(self) -> dict[str, EnvironmentStack]:
        ENV_FIELDS = ["config", "resources", "variables"]

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

    def to_pulumi(self, env=None):
        if env is not None and env in self.envs:
            env = self.envs[env]
        else:
            env = self

        # Resources
        resources = {}
        for r in env.resources._all.values():
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        return PulumiStack(
            name=env.name,
            config=env.config,
            description=env.description,
            resources=resources,
            variables=env.variables,
            outputs=env.pulumi_outputs,
        )

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def to_terraform(self):
        raise NotImplementedError()
