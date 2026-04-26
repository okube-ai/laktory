import json
import shutil
from pathlib import Path
from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
    PipelineConfigWorkspaceFile,
)
from laktory.models.pipelinechild import PipelineChild
from laktory.models.resources.databricks.pipeline import Pipeline

logger = get_logger(__name__)


class DatabricksPipelineOrchestrator(Pipeline, PipelineChild):
    """
    Databricks Pipeline used as an orchestrator to execute a Laktory pipeline.

    DLT orchestrator does not support pipeline nodes with views (as opposed to
    materialized tables). Also, it does not support writing to multiple
    schemas within the same pipeline.

    Selecting this orchestrator requires to add the supporting
    [notebook](https://github.com/okube-ai/laktory/blob/main/laktory/resources/quickstart-stacks/workflows/notebooks/dlt/dlt_laktory_pl.py)
    to the stack.

    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    * [Databricks DLT](https://www.databricks.com/product/delta-live-tables)
    """

    type: Literal["DATABRICKS_PIPELINE"] = Field(
        "DATABRICKS_PIPELINE", description="Type of orchestrator"
    )
    config_file: PipelineConfigWorkspaceFile = Field(
        PipelineConfigWorkspaceFile(),
        description="Pipeline configuration (json) file deployed to the workspace and used by the job to read and execute the pipeline.",
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
                    s.catalog_name = s.catalog_name or self.catalog
                    s.schema_name = s.schema_name or self.schema_ or self.target

        # Update pipeline config
        _requirements = self.inject_vars_into_dump({"deps": pl._dependencies})["deps"]
        _path = (
            "/Workspace"
            + self.inject_vars_into_dump({"path": self.config_file.path})["path"]
        )
        if self.configuration is None:
            self.configuration = {}
        self.configuration["pipeline_name"] = pl.name  # only for reference
        self.configuration["requirements"] = json.dumps(_requirements)
        self.configuration["config_filepath"] = _path
        # This is to ensure configuration is flagged as set and part of
        # model_fields_set when injecting variables.
        self.configuration = self.configuration

    # ----------------------------------------------------------------------- #
    # DABs                                                                    #
    # ----------------------------------------------------------------------- #

    def to_dab_resource(self):
        """
        Convert to a DABs Python Pipeline resource object for use with
        ``laktory.dab.build_resources``.

        Returns
        -------
        :
            ``databricks.bundles.pipelines.Pipeline`` instance.
        """
        from databricks.bundles.pipelines import Pipeline as DabsPipeline

        d = self.model_dump(
            exclude=self.terraform_excludes, exclude_unset=True, by_alias=False
        )
        # schema_ is a Python workaround for the reserved name; DABs expects "schema"
        if "schema_" in d:
            d["schema"] = d.pop("schema_")

        # Pipeline notebook
        source_filepath = (
            Path(__file__).parent.parent.parent.parent
            / "resources"
            / "quickstart-stacks"
            / "workflows"
            / "workspacefiles"
            / "notebooks"
            / "dlt_laktory_pl.py"
        )
        target_filepath = Path(settings.build_root) / "pipelines" / "dlt_laktory_pl.py"
        shutil.copy(source_filepath, target_filepath)

        # Laktory pipelines use a common notebook (copied above). Its path is
        # hardcoded here, but should probably be hardcoded in the base resource as well.
        # TODO: Allow for other libraries?
        notebook_filepath = (
            Path("/Workspace" + settings.workspace_root)
            / "pipelines"
            / "dlt_laktory_pl"
        )
        d["libraries"] = [{"notebook": {"path": notebook_filepath.as_posix()}}]

        return DabsPipeline.from_dict(d)

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["config_file", "type"]

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        return "dlt-pipeline"

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        excludes = dict(super().terraform_excludes)
        excludes["config_file"] = True
        excludes["type"] = True
        excludes["dataframe_backend"] = True
        excludes["dataframe_api"] = True
        return excludes

    @property
    def additional_core_resources(self) -> list:
        """
        - configuration workspace file
        - configuration workspace file permissions
        """

        resources = super().additional_core_resources
        resources += [self.config_file]

        return resources
