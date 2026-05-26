import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Literal

import yaml
from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory._logger import get_logger
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


class SparkDeclarativePipelineOrchestrator(PipelineChild):
    """
    Spark Declarative Pipeline used as an orchestrator to execute a Laktory
    pipeline locally (via `spark-pipelines run`) or as a plain Databricks Job
    task on DBR 16.x.

    Generates three artifacts into `build_root/pipelines/`:

    - `laktory_sdp.py` — Python definition script with `@dp.materialized_view` / `@dp.table` decorators
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
    catalog: str | None = Field(
        None, description="The default target catalog for pipeline outputs."
    )
    configuration: dict[str, str] | None = Field(
        None,
        description="Map of configuration properties",
    )
    schema_: str | None = Field(
        None,
        description="The default target schema for pipeline outputs. database can alternatively be used as an alias.",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_", "database"),
    )
    # config_file: PipelineConfigWorkspaceFile = Field(
    #     PipelineConfigWorkspaceFile(),
    #     description="Pipeline configuration (json) file used by laktory_sdp.py to read and execute the pipeline.",
    # )
    storage_: str | None = Field(
        None,
        description="A directory to store checkpoints and configuration files. Must include a scheme (file://, s3a://, hdfs://, etc.). Defaults to file:///{self.parent_pipeline.root_path}.",
        validation_alias=AliasChoices("storage", "storage_"),
        exclude=True,
    )

    @computed_field(description="storage")
    def storage(self) -> str | None:
        if self.storage_:
            return self.storage_

        pl = self.parent_pipeline
        if not pl:
            return None

        return "file://" + str(pl.root_path.absolute())

    @property
    def config_dict(self):
        pl = self.parent_pipeline
        if not pl:
            return None

        return pl.model_dump(exclude_unset=True, mode="json")

    @property
    def config_filepath_abs(self) -> Path:
        return (
            self.parent_pipeline.root_path.absolute()
            / f"{self.parent_pipeline.name}.json"
        )

    @property
    def config_filepath_rel(self) -> Path:
        return Path(
            os.path.relpath(self.config_filepath_abs, self.parent_pipeline.root_path)
        )

    @property
    def spec_dict(self):
        _conf = dict(self.configuration or {})
        _conf["laktory.is_sdp_execute"] = "true"
        _conf["config_filepath"] = str(self.config_filepath_rel)

        return {
            "name": self.parent_pipeline.name,
            "libraries": [{"glob": {"include": "laktory_sdp.py"}}],
            **self.model_dump(
                mode="json",
                exclude_unset=True,
                exclude=[
                    "type",
                    "dataframe_backend",
                    "dataframe_api",
                    "configuration",
                ],
                by_alias=True,
            ),
            "configuration": _conf,
        }

    @property
    def spec_filepath_abs(self) -> Path:
        return self.parent_pipeline.root_path.absolute() / "spark-pipeline.yaml"

    @property
    def spec_filepath_rel(self) -> Path:
        return Path(
            os.path.relpath(self.spec_filepath_abs, self.parent_pipeline.root_path)
        )

    # ----------------------------------------------------------------------- #
    # Update LDP                                                              #
    # ----------------------------------------------------------------------- #

    def update_from_parent(self):
        # TODO: Re-use the same function from the Lakeflow Declarative Pipeline?

        pl = self.parent_pipeline

        for node in pl.nodes:
            if node.is_view:
                raise ValueError(
                    f"Node '{node.name}' of pipeline '{pl.name}' is a view which is not supported with Spark Declarative Pipeline orchestrator."
                )

        for n in pl.nodes:
            for s in n.all_sinks:
                if isinstance(s, TableDataSink):
                    s.catalog_name = s.catalog_name or self.catalog
                    s.schema_name = s.schema_name or self.schema_

        # Update pipeline config
        # _requirements = self.inject_vars_into_dump({"deps": pl._dependencies})["deps"]
        # _path = (
        #     "/Workspace"
        #     + self.inject_vars_into_dump({"path": self.config_file.path})["path"]
        # )
        # if self.configuration is None:
        #     self.configuration = {}
        # self.configuration["pipeline_name"] = pl.name  # only for reference
        # # self.configuration["requirements"] = json.dumps(_requirements)
        # # self.configuration["config_filepath"] = _path
        # # This is to ensure configuration is flagged as set and part of
        # # model_fields_set when injecting variables.
        # self.configuration = self.configuration

    # ----------------------------------------------------------------------- #
    # Build                                                                   #
    # ----------------------------------------------------------------------- #

    def build(self) -> None:
        """
        Generate SDP artifacts into the pipeline root directory.
        """
        pl = self.parent_pipeline

        root_dir = pl.root_path
        root_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing SDP artifacts to {root_dir}")

        # Write laktory pipeline config JSON
        with self.config_filepath_abs.open("w") as fp:
            json.dump(self.config_dict, fp, indent=4)

        # Write Spark pipeline spec file
        with self.spec_filepath_abs.open("w") as fp:
            yaml.dump(self.spec_dict, fp, default_flow_style=False, sort_keys=False)

        # Copy python code
        source_script = (
            Path(__file__).parent.parent.parent.parent
            / "resources"
            / "scripts"
            / "laktory_sdp.py"
        )
        target_script = root_dir / "laktory_sdp.py"
        shutil.copy(source_script, target_script)

    def execute(
        self,
        full_refresh: bool = False,
        selects: list[str] | None = None,
        cwd: str | None = None,
        read_output: bool = False,
    ):
        """
        Build SDP artifacts then run the pipeline via `spark-pipelines run`.

        Parameters
        ----------
        full_refresh:
            If `True` without `selects`, passes `--full-refresh-all` to the CLI
            to reset and recompute all datasets. If combined with `selects`,
            passes `--full-refresh DATASETS` for the specified datasets only.
        selects:
            Comma-joined and passed as `--refresh DATASETS` (or
            `--full-refresh DATASETS` when `full_refresh=True`). Values must be
            dataset names as they appear in the SDP pipeline (i.e. table/view
            names), not Laktory node names.
        cwd:
            Working directory for the subprocess. Controls where `spark-warehouse/`
            is created. Defaults to the pipeline root path.
        read_output:
            If `True` pipeline outputs are read and assigned to respective nodes (`pipeline.nodes[i].output_df`)
        """
        from laktory import get_spark_session

        self.build()
        cmd = ["spark-pipelines", "run", "--spec", str(self.spec_filepath_rel)]
        if cwd is None:
            cwd = self.parent_pipeline.root_path.absolute()

        if full_refresh and selects:
            cmd += ["--full-refresh", ",".join(selects)]
        elif full_refresh:
            cmd += ["--full-refresh-all"]
        elif selects:
            cmd += ["--refresh", ",".join(selects)]

        logger.info(f"Running SDP pipeline: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, cwd=cwd)

        # Read back node outputs from the spark-warehouse written by SDP
        if read_output:
            spark = get_spark_session()
            for node in self.parent_pipeline.nodes:
                for sink in node.sinks:
                    warehouse_root = Path(cwd) / "spark-warehouse"
                    schema_name = sink.schema_name or "default"
                    if schema_name != "default":
                        warehouse_root = warehouse_root / f"{schema_name}.db"

                    if sink.catalog_name:
                        # Unity Catalog — table accessible via catalog directly
                        node._output_df = sink.as_source().read()
                    else:
                        dataframe_path = warehouse_root / sink.table_name
                        if dataframe_path.exists():
                            # Hive tables can be saved as parquet or delta
                            # We use parquet for simplicity because it will work in
                            # both cases
                            node._output_df = spark.read.parquet(str(dataframe_path))

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return []
