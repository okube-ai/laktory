from typing import Union

from laktory._settings import settings
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
    PipelineConfigWorkspaceFile,
)
from laktory.models.pipeline.orchestrators.pipelinerequirementsworkspacefile import (
    PipelineRequirementsWorkspaceFile,
)
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.resources.databricks.dltpipeline import DLTPipeline
from laktory.models.resources.pulumiresource import PulumiResource


class DatabricksDLTOrchestrator(DLTPipeline, PipelineChild):
    """
    Databricks DLT used as an orchestrator to execute a Laktory pipeline.

    DLT orchestrator does not support pipeline nodes with views (as opposed to
    materialized tables). Also, it does not support writing to multiple
    schemas within the same pipeline.

    Selecting this orchestrator requires to add the supporting
    [notebook](https://github.com/okube-ai/laktory/blob/main/laktory/resources/quickstart-stacks/workflows/notebooks/dlt/dlt_laktory_pl.py)
    to the stack.

    Attributes
    ----------
    config_file:
        Pipeline configuration (json) file deployed to the workspace and used
        by the job to read and execute the pipeline.
    requirements_file:
        Pipeline requirements (json) file deployed to the workspace and used
        by the job to install the required python dependencies.
    """

    config_file: PipelineConfigWorkspaceFile = PipelineConfigWorkspaceFile()
    requirements_file: PipelineRequirementsWorkspaceFile = (
        PipelineRequirementsWorkspaceFile()
    )

    # ----------------------------------------------------------------------- #
    # Update DLT                                                              #
    # ----------------------------------------------------------------------- #

    def update_from_parent(self):

        pl = self.parent_pipeline

        for node in pl.nodes:
            if node.is_view:
                raise ValueError(
                    f"Node '{node.name}' of pipeline '{pl.name}' is a view which is not supported with DLT orchestrator."
                )

        for n in pl.nodes:
            for s in n.all_sinks:
                if isinstance(s, TableDataSink):
                    s.catalog_name = self.catalog
                    s.schema_name = self.target

        # Configuration
        self.configuration["pipeline_name"] = pl.name
        self.configuration["workspace_laktory_root"] = settings.workspace_laktory_root

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
        return "dlt-pipeline"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        excludes = super().pulumi_excludes
        excludes["config_file"] = True
        excludes["requirements_file"] = True
        return excludes

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
