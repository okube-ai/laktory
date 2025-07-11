from typing import TYPE_CHECKING

from laktory._useragent import DATABRICKS_USER_AGENT
from laktory._useragent import VERSION
from laktory.dispatcher.databricksjobrunner import DatabricksJobRunner
from laktory.dispatcher.databrickspipelinerunner import DatabricksPipelineRunner
from laktory.models.stacks.stack import Stack

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class Dispatcher:
    """
    The dispatcher is a manager that can be used to run and monitor remote jobs
    and DLT pipelines defined in a stack. It is generally used through Laktory
    CLI `run` command, but may be used directly in scripts and python
    programs.

    Parameters
    ----------
    stack
        Stack object
    env
        Selected environment

    Examples
    --------
    ```py tag:skip-run
    from laktory import Dispatcher
    from laktory import models

    with open("./stack.yaml") as fp:
        stack = models.Stack.model_validate_yaml(fp)

    dispatcher = Dispatcher(stack=stack)
    dispatcher.get_resource_ids()
    pl = dispatcher.resources["pl-stock-prices"]
    job = dispatcher.resources["job-stock-prices"]

    # Run pipeline
    pl.run(current_run_action="WAIT", full_refresh=False)

    # Run job
    job.run(current_run_action="CANCEL")
    ```
    """

    def __init__(self, stack: Stack = None, env: str = None):
        self.stack = stack
        self._env = env
        self._wc = None
        self.resources = {}

        self.init_resources()

    def init_resources(self):
        """Set resource for each of the resources defined in the stack"""

        from laktory.models.pipeline.orchestrators.databricksjoborchestrator import (
            DatabricksJobOrchestrator,
        )
        from laktory.models.pipeline.orchestrators.databrickspipelineorchestrator import (
            DatabricksPipelineOrchestrator,
        )

        for k, pl in self.stack.resources.pipelines.items():
            if not pl.options.is_enabled:
                continue

            if isinstance(pl.orchestrator, DatabricksPipelineOrchestrator):
                self.resources[pl.orchestrator.name] = DatabricksPipelineRunner(
                    dispatcher=self, name=pl.orchestrator.name
                )

            if isinstance(pl.orchestrator, DatabricksJobOrchestrator):
                self.resources[pl.orchestrator.name] = DatabricksJobRunner(
                    dispatcher=self, name=pl.orchestrator.name
                )

        for k, pl in self.stack.resources.databricks_pipelines.items():
            if not pl.options.is_enabled:
                continue
            self.resources[pl.name] = DatabricksPipelineRunner(
                dispatcher=self, name=pl.name
            )

        for k, job in self.stack.resources.databricks_jobs.items():
            if not job.options.is_enabled:
                continue
            self.resources[job.name] = DatabricksJobRunner(
                dispatcher=self, name=job.name
            )

    # ----------------------------------------------------------------------- #
    # Environment                                                             #
    # ----------------------------------------------------------------------- #

    @property
    def env(self) -> str:
        """Selected environment"""
        return self._env

    @env.setter
    def env(self, value):
        """Set environment"""
        self._env = value
        self._wc = None

    # ----------------------------------------------------------------------- #
    # Workspace Client                                                        #
    # ----------------------------------------------------------------------- #

    @property
    def _workspace_arguments(self):
        data = {}
        if self.stack.backend == "pulumi":
            config = self.stack.to_pulumi(env_name=self.env).model_dump()["config"]
            for k, v in config.items():
                if k.startswith("databricks"):
                    _k = k.split(":")[1]
                    data[_k] = v
        elif self.stack.backend == "terraform":
            providers = self.stack.to_terraform(env_name=self.env).model_dump()[
                "provider"
            ]
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
    def wc(self) -> "WorkspaceClient":
        """Databricks Workspace Client"""
        from databricks.sdk import WorkspaceClient

        if self._wc is None:
            self._wc = WorkspaceClient(**self._workspace_arguments)
            self._wc.config.with_user_agent_extra(
                key=DATABRICKS_USER_AGENT,
                value=VERSION,
            )
        return self._wc

    # ----------------------------------------------------------------------- #
    # Resources                                                               #
    # ----------------------------------------------------------------------- #

    def get_resource_ids(self, env=None):
        """
        Get resource ids for each of the resources defined in the stack in the
        provided environment `env`.
        """
        if env is not None:
            self.env = env

        for r in self.resources.values():
            r.get_id()

    # ----------------------------------------------------------------------- #
    # Run                                                                     #
    # ----------------------------------------------------------------------- #

    def run_databricks_job(self, job_name: str, *args, **kwargs):
        """
        Run job with name `job_name`

        Parameters
        ----------
        job_name:
            Name of the job
        *args:
            Arguments passed to `JobRunner.run()`
        **kwargs:
            Keyword arguments passed to `JobRunner.run()`
        """
        job = self.resources[job_name]
        job.run(*args, **kwargs)

    def run_databricks_dlt(self, dlt_name: str, *args, **kwargs):
        """
        Run Databricks pipeline with name `dlt_name`

        Parameters
        ----------
        dlt_name:
            Name of the DLT pipeline
        *args:
            Arguments passed to `JobRunner.run()`
        **kwargs:
            Keyword arguments passed to `JobRunner.run()`
        """
        pl = self.resources[dlt_name]
        pl.run(*args, **kwargs)
