import os
import yaml
from typing import Union

from laktory._logger import get_logger
from laktory._worker import Worker
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
    catalogs: list[Catalog] = []
    clusters: list[Cluster] = []
    groups: list[Group] = []
    jobs: list[Job] = []
    notebooks: list[Notebook] = []
    pipelines: list[Pipeline] = []
    schemas: list[Schema] = []
    secrets: list[Secret] = []
    secretscopes: list[SecretScope] = []
    serviceprincipals: list[ServicePrincipal] = []
    sqlqueries: list[SqlQuery] = []
    tables: list[Table] = []
    providers: list[Union[DatabricksProvider]] = []
    users: list[User] = []
    volumes: list[Volume] = []
    warehouses: list[Warehouse] = []
    workspacefiles: list[WorkspaceFile] = []


class StackEnvironment(BaseModel):
    pass


class StackVariable(BaseModel):
    pass


class Stack(BaseModel):
    """
    The Stack defines a group of deployable resources.
    """

    name: str
    config: dict[str, str] = None
    description: str = None
    resources: StackResources
    environments: list[StackEnvironment] = []
    variables: dict[str, str] = {}
    pulumi_outputs: dict[str, str] = {}  # TODO

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    def to_pulumi_stack(self):
        resources = {}

        for resource_type in self.resources.model_fields.keys():
            if resource_type in ["variables"]:
                continue

            for r in getattr(self.resources, resource_type):
                for _r in r.resources:
                    resources[_r.resource_name] = _r

        return PulumiStack(
            name=self.name,
            config=self.config,
            description=self.description,
            resources=resources,
            variables=self.variables,
            outputs=self.pulumi_outputs,
        )

    def write_pulumi_stack(self) -> str:
        # TODO: Write environment configs
        filepath = os.path.join(CACHE_ROOT, "Pulumi.yaml")

        if not os.path.exists(CACHE_ROOT):
            os.makedirs(CACHE_ROOT)

        with open(filepath, "w") as fp:
            yaml.dump(self.to_pulumi_stack().model_dump(), fp)

        return filepath

    def _pulumi_call(self, command, stack=None, flags=None):
        filepath = self.write_pulumi_stack()
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
