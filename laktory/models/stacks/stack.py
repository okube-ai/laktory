import os
import yaml
from collections import defaultdict
from typing import Union
from typing import Any
from pydantic import model_validator
from pydantic import Field

from laktory._logger import get_logger
from laktory._worker import Worker
from laktory._parsers import merge_dicts
from laktory.constants import CACHE_ROOT
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
    tables: dict[str, Catalog] = {}
    providers: dict[str, Union[DatabricksProvider]] = {}
    users: dict[str, User] = {}
    volumes: dict[str, Volume] = {}
    warehouses: dict[str, Warehouse] = {}
    workspacefiles: dict[str, WorkspaceFile] = {}


class EnvironmentStack(BaseModel):
    config: dict[str, str] = {}
    description: str = None
    name: str
    pulumi_outputs: dict[str, str] = {}
    resources: StackResources = StackResources()
    variables: dict[str, Union[str, bool]] = {}

    @property
    def all_resources(self):

        resources = {}
        for resource_type in self.resources.model_fields.keys():

            if resource_type in ["variables"]:
                continue

            for resource_name, _r in getattr(self.resources, resource_type).items():
                resources[resource_name] = _r

        return resources


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
    pulumi_outputs: dict[str, str] = {}
    resources: StackResources = StackResources()
    variables: dict[str, Union[str, bool]] = {}
    environments: dict[str, EnvironmentSettings] = {}
    envs: dict[str, EnvironmentStack] = Field({}, exclude=True)

    # ----------------------------------------------------------------------- #
    # Validators                                                              #
    # ----------------------------------------------------------------------- #

    @model_validator(mode='before')
    def parse_environments(cls, data: Any) -> Any:

        if "environments" not in data:
            return data

        ENV_FIELDS = ["config", "resources", "variables"]

        data["envs"] = defaultdict(lambda: {})
        for env_name, env in data["environments"].items():

            # Merge
            for k in Stack.model_fields.keys():

                # Skip
                if k in ["environments", "envs"]:
                    continue

                # Merge
                elif k in ENV_FIELDS:
                    data["envs"][env_name][k] = merge_dicts(data.get(k, {}), env.get(k, {}))

                # Overwrite
                else:
                    if k in data:
                        data["envs"][env_name][k] = data[k]

        return data

    @model_validator(mode="after")
    def update_resource_names(self) -> Any:
        for env in [self] + list(self.envs.values()):
            for k, r in env.all_resources.items():
                if r.resource_name_ and k != r.resource_name_:
                    raise ValueError(f"Provided resource name {r.resource_name_} does not match provided key {k}")
                r.resource_name_ = k
        return self

    all_resources = EnvironmentStack.all_resources

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    def to_pulumi_stack(self, env=None):

        if env is not None:
            env = self.envs[env]
        else:
            env = self

        return PulumiStack(
            name=env.name,
            config=env.config,
            description=env.description,
            resources=env.all_resources,
            variables=env.variables,
            outputs=env.pulumi_outputs,
        )

    def write_pulumi_stack(self, env=None) -> str:
        filepath = os.path.join(CACHE_ROOT, "Pulumi.yaml")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        with open(filepath, "w") as fp:
            yaml.dump(self.to_pulumi_stack(env).model_dump(), fp)

        return filepath

    def _pulumi_call(self, command, stack=None, flags=None):
        env = None
        if stack is not None:
            env = stack.split("/")[-1]
        filepath = self.write_pulumi_stack(env)
        worker = Worker()

        cmd = ["pulumi", command]

        # Stack
        if stack is not None:
            cmd += ["-s", stack]

        if flags is not None:
            cmd += flags

        worker.run(
            cmd=cmd,
            cwd=CACHE_ROOT,
        )

    def pulumi_preview(self, stack=None, flags=None):
        self._pulumi_call("preview", stack=stack, flags=flags)

    def pulumi_up(self, stack=None, flags=None):
        self._pulumi_call("up", stack=stack, flags=flags)

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def model_terraform_dump(self):
        pass
