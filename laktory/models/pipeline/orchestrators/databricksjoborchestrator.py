from typing import Union

from laktory._settings import settings
from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
    PipelineConfigWorkspaceFile,
)
from laktory.models.pipeline.orchestrators.pipelinerequirementsworkspacefile import (
    PipelineRequirementsWorkspaceFile,
)
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.resources.databricks.cluster import ClusterLibrary
from laktory.models.resources.databricks.cluster import ClusterLibraryPypi
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.job import JobParameter
from laktory.models.resources.databricks.job import JobTask
from laktory.models.resources.pulumiresource import PulumiResource


class DatabricksJobOrchestrator(Job, PipelineChild):
    """
    Databricks job used as an orchestrator to execute a Laktory pipeline.

    Job orchestrator supports incremental workloads with Spark Structured
    Streaming, but it does not support continuous processing.

    Selecting this orchestrator requires to add the supporting
    [notebook](https://github.com/okube-ai/laktory/blob/main/laktory/resources/quickstart-stacks/workflows/notebooks/jobs/job_laktory_pl.py)
    to the stack.

    Attributes
    ----------
    notebook_path:
        Path for the notebook. If `None`, default path for laktory job notebooks is used.
    config_file:
        Pipeline configuration (json) file deployed to the workspace and used
        by the job to read and execute the pipeline.
    requirements_file:
        Pipeline requirements (json) file deployed to the workspace and used
        by the job to install the required python dependencies.
    """

    notebook_path: Union[str, None] = None
    config_file: PipelineConfigWorkspaceFile = PipelineConfigWorkspaceFile()
    requirements_file: PipelineRequirementsWorkspaceFile = (
        PipelineRequirementsWorkspaceFile()
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

        self.parameters = [
            JobParameter(name="full_refresh", default="false"),
            JobParameter(name="pipeline_name", default=pl.name),
            JobParameter(
                name="install_dependencies", default=str(not cluster_found).lower()
            ),
        ]

        notebook_path = self.notebook_path
        if notebook_path is None:
            notebook_path = f"{settings.workspace_laktory_root}jobs/job_laktory_pl.py"

        self.tasks = []

        # Sorting Node Names to prevent job update trigger with Pulumi
        node_names = [node.name for node in pl.nodes]
        node_names.sort()
        for node_name in node_names:

            node = pl.nodes_dict[node_name]

            depends_on = []
            for edge in pl.dag.in_edges(node.name):
                depends_on += [{"task_key": "node-" + edge[0]}]

            libraries = []
            if cluster_found:
                libraries = [
                    ClusterLibrary(pypi=ClusterLibraryPypi(package=d))
                    for d in pl._dependencies
                ]

            self.tasks += [
                JobTask(
                    task_key="node-" + node.name,
                    notebook_task={
                        "base_parameters": {"node_name": node.name},
                        "notebook_path": notebook_path,
                    },
                    libraries=libraries,
                    depends_ons=depends_on,
                )
            ]
            self.sort_tasks(self.tasks)

            if cluster_found:
                self.tasks[-1].job_cluster_key = "node-cluster"

        # Config file
        self.config_file.update_from_parent()

        # Requirements file
        self.requirements_file.update_from_parent()

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def child_attribute_names(self):
        return ["config_file", "requirements_file"]

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        return "job"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return super().pulumi_excludes + [
            "notebook_path",
            "config_file",
            "requirements_file",
        ]

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - configuration workspace file
        - configuration workspace file permissions
        - requirements workspace file
        - requirements workspace file permissions
        """

        resources = super().additional_core_resources
        resources += [self.config_file]
        resources[-1].write_source()
        resources += [self.requirements_file]
        resources[-1].write_source()

        return resources
