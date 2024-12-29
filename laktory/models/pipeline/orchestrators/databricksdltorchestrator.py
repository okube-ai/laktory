from typing import Union

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
    Databricks DLT used as an orchestrator to execute a Laktory pipeline

    Attributes
    ----------

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
        """ """
        return "dlt"  # For backward compatibility

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

        resources = []
        resources += [self.config_file]
        resources[-1].write_source()
        resources += [self.requirements_file]
        resources[-1].write_source()

        return resources
