from typing import Union

from laktory._settings import settings
from laktory.models.pipeline.orchestrators.databricksconfigfile import (
    PipelineConfigWorkspaceFile,
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
    Databricks job used as an orchestrator to execute a Laktory pipeline

    Attributes
    ----------
    notebook_path:
        Path for the notebook. If `None`, default path for laktory job notebooks is used.

    """

    notebook_path: Union[str, None] = None
    config_file: PipelineConfigWorkspaceFile = PipelineConfigWorkspaceFile()
    requirements_file: str = None

    # ----------------------------------------------------------------------- #
    # Update Job                                                              #
    # ----------------------------------------------------------------------- #

    def update_from_parent(self):

        cluster_found = False
        for c in self.clusters:
            if c.name == "node-cluster":
                cluster_found = True
        if not cluster_found:
            raise ValueError(
                "To use DATABRICKS_JOB orchestrator, a cluster named `node-cluster` must be defined in the databricks_job attribute."
            )

        pl = self.parent_pipeline

        # # Libraries
        # pypi_packages = []
        # for p in pl.pypi_packages:
        #     pypi_packages += [p]

        self.parameters = [
            JobParameter(name="full_refresh", default="false"),
            JobParameter(name="pipeline_name", default=pl.name),
            JobParameter(name="install_dependencies", default=str(not cluster_found).lower()),
            # JobParameter(name="pypi_packages", default=json.dumps(pypi_packages)),
            # JobParameter(name="pipeline_path", default=self.workspacefile.path),
            # JobParameter(name="workspace_laktory_root", default=settings.workspace_laktory_root),
            # JobParameter(name="laktory_root", default=settings.workspace_laktory_root),
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

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def child_attribute_names(self):
        return ["config_file"]

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """ """
        return "pipeline-databricks-job"  # For backward compatibility

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

        resources = []

        resources += [self.config_file]
        resources[-1].write_source(self.parent_pipeline)

        return resources
