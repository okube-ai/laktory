from databricks.sdk import WorkspaceClient

from laktory.models.stacks.stack import Stack
from laktory.dispatcher.pipelinerunner import PipelineRunner
from laktory.dispatcher.jobrunner import JobRunner


class Dispatcher:
    def __init__(self, stack: Stack = None, env: str = None):
        self.stack = stack
        self._env = env
        self._wc = None
        self.resources = {}

        self.init_resources()

    def init_resources(self):
        for k, pl in self.stack.resources.pipelines.items():
            self.resources[k] = PipelineRunner(dispatcher=self, name=pl.name)

        for k, job in self.stack.resources.jobs.items():
            self.resources[k] = JobRunner(dispatcher=self, name=job.name)

    # ----------------------------------------------------------------------- #
    # Environment                                                             #
    # ----------------------------------------------------------------------- #

    @property
    def env(self):
        return self._env

    @env.setter
    def env(self, value):
        self._env = value
        self._wc = None

    # ----------------------------------------------------------------------- #
    # Workspace Client                                                        #
    # ----------------------------------------------------------------------- #

    @property
    def workspace_arguments(self):
        data = {}
        if self.stack.backend == "pulumi":
            config = self.stack.to_pulumi(env=self.env).model_dump()["config"]
            for k, v in config.items():
                if k.startswith("databricks"):
                    _k = k.split(":")[1]
                    data[_k] = v
        elif self.stack.backend == "terraform":
            providers = self.stack.to_terraform(env=self.env).model_dump()["provider"]
            for k in providers:
                if "databricks" in k.lower():
                    data = providers[k]
                    break

        kwargs = {}
        for k in [
            "host",
            "account_id",
            "username",
            "password",
            "client_id",
            "client_secret",
            "token",
            "profile",
            "config_file",
            "azure_workspace_resource_id",
            "azure_client_secret",
            "azure_client_id",
            "azure_tenant_id",
            "azure_environment",
            "auth_type",
            "cluster_id",
            "google_credentials",
            "google_service_account",
            "debug_truncate_bytes",
            "debug_headers",
            "product",
            "product_version",
        ]:
            if k in data:
                kwargs[k] = data[k]

        return kwargs

    @property
    def wc(self) -> WorkspaceClient:
        if self._wc is None:
            self._wc = WorkspaceClient(**self.workspace_arguments)
        return self._wc

    # ----------------------------------------------------------------------- #
    # Resources                                                               #
    # ----------------------------------------------------------------------- #

    def get_resource_ids(self, env=None):
        if env is not None:
            self.env = env

        for r in self.resources.values():
            r.get_id()
