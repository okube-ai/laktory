import shutil
import subprocess
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
    PipelineConfigWorkspaceFile,
)
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


class SparkDeclarativePipelineOrchestrator(PipelineChild):
    """
    Spark Declarative Pipeline used as an orchestrator to execute a Laktory
    pipeline locally (via `spark-pipelines run`) or as a plain Databricks Job
    task on DBR 16.x (no DLT/Lakeflow license required).

    Generates three artifacts into `build_root/pipelines/`:

    - `sdp_laktory_pl.py` — Python definition script with `@dp.materialized_view` / `@dp.table` decorators
    - `{pipeline_name}.json` — serialized pipeline configuration
    - `{pipeline_name}-spec.yml` — SDP YAML spec pointing to the script and config

    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    * [Spark Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines.html)
    """

    type: Literal["SPARK_DECLARATIVE_PIPELINE"] = Field(
        "SPARK_DECLARATIVE_PIPELINE", description="Type of orchestrator"
    )
    config_file: PipelineConfigWorkspaceFile = Field(
        PipelineConfigWorkspaceFile(),
        description="Pipeline configuration (json) file used by sdp_laktory_pl.py to read and execute the pipeline.",
    )
    storage: str | None = Field(
        None,
        description="URI for SDP checkpoint storage. Must include a scheme (file://, s3a://, hdfs://, etc.). Defaults to file:///tmp/{pipeline_safe_name}-sdp-checkpoints.",
    )

    # ----------------------------------------------------------------------- #
    # Build                                                                    #
    # ----------------------------------------------------------------------- #

    def build(self) -> Path:
        """
        Generate SDP artifacts into `build_root/pipelines/`.

        Returns
        -------
        :
            Path to the generated `{pipeline_name}-spec.yml` file.
        """
        pl = self.parent_pipeline

        pipelines_dir = Path(settings.build_root) / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)

        # 1. Write pipeline config JSON
        self.config_file.build()

        # 2. Copy sdp_laktory_pl.py template
        source_script = (
            Path(__file__).parent.parent.parent.parent
            / "resources"
            / "quickstart-stacks"
            / "workflows"
            / "workspacefiles"
            / "notebooks"
            / "sdp_laktory_pl.py"
        )
        target_script = pipelines_dir / "sdp_laktory_pl.py"
        shutil.copy(source_script, target_script)

        # 3. Write SDP spec YAML
        config_path = pipelines_dir / f"{pl.name}.json"
        storage = self.storage or f"file:///tmp/{pl.safe_name}-sdp-checkpoints"
        spec = {
            "name": pl.name,
            "libraries": [{"glob": {"include": "sdp_laktory_pl.py"}}],
            "storage": storage,
            "configuration": {
                "laktory.is_sdp_execute": "true",
                "config_filepath": str(config_path),
            },
        }
        spec_path = pipelines_dir / f"{pl.name}-spec.yml"
        logger.debug(f"Writing SDP spec at {spec_path}")
        with open(spec_path, "w") as fp:
            yaml.dump(spec, fp, default_flow_style=False, sort_keys=False)

        return spec_path

    def execute(self, full_refresh: bool = False, cwd: str | None = None):
        """
        Build SDP artifacts then run the pipeline via `spark-pipelines run`.

        Parameters
        ----------
        full_refresh:
            If `True`, passes `--full-refresh` to the CLI.
        cwd:
            Working directory for the subprocess. Controls where `spark-warehouse/`
            is created. Defaults to the current process working directory.
        """
        spec_path = self.build()
        cmd = ["spark-pipelines", "run", "--spec", str(spec_path)]
        if full_refresh:
            cmd += ["--full-refresh"]
        logger.info(f"Running SDP pipeline: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, cwd=cwd)

    # ----------------------------------------------------------------------- #
    # Children                                                                 #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["config_file", "type"]
