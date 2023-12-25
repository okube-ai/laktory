import os
import yaml

from laktory.models.basemodel import BaseModel
from laktory.models.stacks.basestack import BaseStack
from laktory.models.databricks.cluster import Cluster
from laktory.models.databricks.group import Group
from laktory.models.databricks.job import Job
from laktory.models.databricks.notebook import Notebook
from laktory.models.databricks.pipeline import Pipeline
from laktory.models.databricks.secretscope import SecretScope
from laktory.models.databricks.sqlquery import SqlQuery
from laktory.models.databricks.user import User
from laktory.models.databricks.warehouse import Warehouse
from laktory.models.databricks.workspacefile import WorkspaceFile
from laktory.models.sql.catalog import Catalog
from laktory.models.sql.schema import Schema
from laktory.models.sql.table import Table
from laktory.models.stacks.pulumistack import PulumiStack
from laktory._worker import Worker
from laktory._logger import get_logger

logger = get_logger(__name__)


class StackResources(BaseModel):
    catalogs: list[Catalog] = []
    cluster: list[Cluster] = []
    groups: list[Group] = []
    jobs: list[Job] = []
    notebooks: list[Notebook] = []
    pipelines: list[Pipeline] = []
    schemas: list[Schema] = []
    secret_scopes: list[SecretScope] = []
    sql_queries: list[SqlQuery] = []
    tables: list[Table] = []
    users: list[User] = []
    warehouse: list[Warehouse] = []
    workspace_files: list[WorkspaceFile] = []


class StackEnvironment(BaseModel):
    pass


class StackVariable(BaseModel):
    pass


class Stack(BaseStack):
    """
    The Stack defines a group of deployable resources.
    """
    name: str
    description: str = None
    resources: StackResources
    environments: list[StackEnvironment] = []
    variables: dict[str, str] = {}
    pulumi_outputs: dict[str, str] = {}  # TODO

    # @property
    # def parsed_resources(self):
    #     resources = self.resources

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    def to_pulumi_stack(self):

        resources = {}

        for r in self.resources.notebooks + self.resources.jobs + self.resources.pipelines:
            for _r in r.all_resources:
                resources[_r.resource_name] = _r.pulumi_yaml_dump()

        return PulumiStack(
            name=self.name,
            description=self.description,
            resources=resources,
            # variables=None,  # TODO
            # config=None,  # TODO
            outputs=self.pulumi_outputs,
        )

    def write_pulumi_stack(self) -> None:
        dirpath = "./.laktory/"

        # TODO: Write environment configs
        filepath = os.path.join(dirpath, "Pulumi.yaml")

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        with open(filepath, "w") as fp:
            yaml.dump(self.to_pulumi_stack().model_dump(), fp)

    def _pulumi_call(self, command, stack=None):
        self.write_pulumi_stack()
        worker = Worker()

        cmd = ["pulumi", command]
        if stack is not None:
            cmd += ["-s", stack]

        worker.run(
            cmd=cmd,
            cwd="./.laktory/",
        )

    def pulumi_preview(self, stack=None):
        self._pulumi_call("preview", stack=stack)

    def pulumi_up(self, stack=None):
        self._pulumi_call("up", stack=stack)

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def model_terraform_dump(self):
        pass
