import json
from typing import Literal

from pydantic import Field
from requests.packages import package

from laktory._settings import settings
from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
    PipelineConfigWorkspaceFile,
)
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.resources.databricks.cluster import ClusterLibrary
from laktory.models.resources.databricks.cluster import ClusterLibraryPypi
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.job import JobEnvironment
from laktory.models.resources.databricks.job import JobEnvironmentSpec
from laktory.models.resources.databricks.job import JobParameter
from laktory.models.resources.databricks.job import JobTask
from laktory.models.resources.databricks.job import JobTaskPythonWheelTask
from laktory.models.resources.pulumiresource import PulumiResource


class DatabricksJobOrchestrator(Job, PipelineChild):
    """
    Databricks job used as an orchestrator to execute a Laktory pipeline.

    Job orchestrator supports incremental workloads with Spark Structured
    Streaming, but it does not support continuous processing.

    Selecting this orchestrator requires to add the supporting
    [notebook](https://github.com/okube-ai/laktory/blob/main/laktory/resources/quickstart-stacks/workflows/notebooks/jobs/job_laktory_pl.py)
    to the stack.


    References
    ----------
    * [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)

    """

    type: Literal["DATABRICKS_JOB"] = Field(
        "DATABRICKS_JOB", description="Type of orchestrator"
    )
    notebook_path: str = Field(
        None,
        description="Path for the notebook. If `None`, default path for laktory job notebooks is used.",
    )
    config_file: PipelineConfigWorkspaceFile = Field(
        PipelineConfigWorkspaceFile(),
        description="Pipeline configuration (json) file deployed to the workspace and used by the job to read and execute the pipeline.",
    )
    node_max_retries: int = Field(
        None,
        description="An optional maximum number of times to retry an unsuccessful run for each node.",
    )

    # ----------------------------------------------------------------------- #
    # Update Job                                                              #
    # ----------------------------------------------------------------------- #

    def update_from_parent(self):
        cluster_found = False
        for c in self.clusters:
            if c.name == "node-cluster":
                cluster_found = True
        if len(self.clusters) > 0 and not cluster_found:
            raise ValueError(
                "To use DATABRICKS_JOB orchestrator, a cluster named `node-cluster` must be defined in the databricks_job attribute."
            )

        pl = self.parent_pipeline

        notebook_path = self.notebook_path
        if notebook_path is None:
            notebook_path = f"{settings.workspace_laktory_root}jobs/job_laktory_pl.py"

        self.tasks = []

        def _get_depends_on(node, pl):
            depends_on = []
            for edge in pl.dag.in_edges(node.name):
                _node = pl.nodes_dict[edge[0]]
                if _node.has_sinks:
                    depends_on += [{"task_key": "node-" + edge[0]}]
                else:
                    depends_on += _get_depends_on(_node, pl)
            return depends_on


        # Environment
        env_key = "laktory"
        env_found = False
        envs = self.environments
        if envs is None:
            envs = []
        for env in envs:
            if env.environment_key == env_key:
                env_found = True
                break
        if not env_found:
            envs += [
                JobEnvironment(
                    environment_key=env_key,
                    spec=JobEnvironmentSpec(
                        client="3",
                        dependencies=pl._dependencies,
                    ),
                )
            ]
            self.environments = envs


        # Sorting Node Names to prevent job update trigger with Pulumi
        node_names = [node.name for node in pl.nodes]
        node_names.sort()
        for node_name in node_names:
            node = pl.nodes_dict[node_name]

            if not node.has_sinks:
                continue

            depends_on = _get_depends_on(node, pl=pl)

            task = JobTask(
                task_key="node-" + node.name,
                python_wheel_task=JobTaskPythonWheelTask(
                    entry_point="print_version",
                    package_name="laktory",
                    # parameters=["version"]
                ),
                depends_ons=depends_on,
            )
            if cluster_found:
                libraries = [
                    ClusterLibrary(pypi=ClusterLibraryPypi(package=d))
                    for d in pl._dependencies
                ]
                task.job_cluster_key = "node-cluster"
                task.libraries = libraries
            else:
                task.environment_key = env_key
                pass
                # TODO: Notebook tasks don't currently support environments. To Enable when they do

            if self.node_max_retries:
                task.max_retries = self.node_max_retries

            self.tasks += [task]

        self.sort_tasks(self.tasks)

        # Update job parameters
        _requirements = self.inject_vars_into_dump({"deps": pl._dependencies})["deps"]
        _path = (
            "/Workspace"
            + self.inject_vars_into_dump({"path": self.config_file.path_})["path"]
        )
        self.parameters = [
            JobParameter(name="full_refresh", default="false"),
            JobParameter(name="pipeline_name", default=pl.name),
            JobParameter(
                name="install_dependencies", default=str(not cluster_found).lower()
            ),
            JobParameter(name="requirements", default=json.dumps(_requirements)),
            JobParameter(name="config_filepath", default=_path),
        ]

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["config_file"]

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        return "job"

    @property
    def pulumi_excludes(self) -> list[str] | dict[str, bool]:
        return super().pulumi_excludes + [
            "notebook_path",
            "config_file",
            "node_max_retries",
            "type",
        ]

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - configuration workspace file
        - configuration workspace file permissions
        """

        resources = super().additional_core_resources
        resources += [self.config_file]

        return resources
