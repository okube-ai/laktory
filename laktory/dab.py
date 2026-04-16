import os
import shutil
from pathlib import Path

from laktory._logger import get_logger
from laktory._settings import DEFAULT_BUILD_ROOT
from laktory._settings import DEFAULT_WORKSPACE_ROOT

logger = get_logger(__name__)


def build_resources(bundle):
    """
    DABs Python entry point for building and loading Laktory pipeline resources.

    This function is called by the Databricks CLI during bundle resolution.
    It discovers Laktory pipeline YAML files, writes their JSON config files to
    disk (for DABs to sync to the workspace), and returns Job and DLT Pipeline
    resources as a DABs ``Resources`` object.

    Two global settings are configured automatically when not already set:

    - ``LAKTORY_BUILD_ROOT`` defaults to ``./laktory/.build/`` relative
      to the bundle root (the directory containing ``databricks.yml``).
    - ``WORKSPACE_LAKTORY_ROOT`` is derived from the
      ``dab_workspace_root`` bundle variable as
      ``{dab_workspace_root}/files/{build_root}/``.

    Examples
    --------
    To use, declare in ``databricks.yml``:

    ```yaml
    variables:
      laktory_pipelines_dir:
        default: ./laktory/pipelines   # comma-separated for multiple dirs
      dab_workspace_root:
        default: ${workspace.root_path}

    sync:
      paths:
        - ./laktory/
      include:
        - ./laktory/.build/**  # needed if laktory/.build/ is in .gitignore

    python:
      venv_path: .venv
      resources:
        - 'laktory.dab:build_resources'
    ```

    Parameters
    ----------
    bundle:
        DABs Bundle object provided by the Databricks CLI.

    Returns
    -------
    :
        DABs Resources object containing all pipeline/job definitions.
    """
    from databricks.bundles.core import Resources

    from laktory._settings import settings
    from laktory.models.pipeline.pipeline import Pipeline

    # Get Bundle (databricks.yml) directory. This only works if CLI is called from the
    # same directory (i.e. --bundle-dir is not used)
    # TODO: build a more reliable approach.
    bundle_dirpath = Path(os.getcwd())

    # Build Root
    if settings.build_root == DEFAULT_BUILD_ROOT:
        settings.build_root = str(bundle_dirpath / "laktory" / ".build")
    logger.info(
        f"Setting `build_root` to default '{settings.build_root}'. Make sure this path is added to Bundle sync paths."
    )

    # Workspace root
    # This is where Laktory files (pipeline config, queries, dashboards, etc.) are
    # deployed. When using Laktory only, default is /Workspace/.laktory/. In
    # the context of DAB, we set it to {dab_workspace_root}/laktory/.build/
    # Unfortunately, {dab_workspace_root} is not available unless the user
    # adds it to the variables.
    dab_workspace_root = bundle.variables.get("dab_workspace_root")
    if settings.workspace_root == DEFAULT_WORKSPACE_ROOT:
        if dab_workspace_root is None:
            raise ValueError(
                "Variable `dab_workspace_root` must be set to '${workspace.root_path}' in databricks.yml to use Laktory."
            )

        # Build Path relative to Bundle root
        build_root_abs = settings.build_root
        build_root_rel = os.path.relpath(build_root_abs, bundle_dirpath)
        settings.workspace_root = f"{dab_workspace_root}/files/{build_root_rel}/"

    # Laktory expect the workspace root to exclude "/Workspace/"
    settings.workspace_root = settings.workspace_root.replace("/Workspace/", "/")

    # Clean the build directory to remove stale files from deleted pipelines
    build_dir = Path(settings.build_root)
    if build_dir.exists():
        shutil.rmtree(build_dir)
        logger.info(f"Cleaned stale build directory '{build_dir}'")
    build_dir.mkdir(parents=True, exist_ok=True)

    # --- Bundle variables ---
    # Expose all bundle variables for injection into pipeline models.
    bundle_vars = {k: v for k, v in bundle.variables.items() if v is not None}

    # --- Discover pipeline YAML files ---
    dirs_raw = bundle_vars.get("laktory_pipelines_dir", "laktory/pipelines")
    pipelines_dirs = [d.strip() for d in dirs_raw.split(",")]

    resources = Resources()

    for laktory_pipelines_dir in pipelines_dirs:
        dirpath = Path(laktory_pipelines_dir)
        if not dirpath.is_absolute():
            dirpath = bundle_dirpath / dirpath

        if not dirpath.exists():
            logger.warning(f"Pipelines directory '{dirpath}' does not exist. Skipping.")
            continue

        yaml_files = sorted(dirpath.glob("*.yaml")) + sorted(dirpath.glob("*.yml"))
        if not yaml_files:
            logger.warning(f"No pipeline YAML files found in '{dirpath}'.")
            continue

        for yaml_file in yaml_files:
            logger.info(f"Loading pipeline from '{yaml_file}'")
            with open(yaml_file, "r", encoding="utf-8") as fp:
                pl = Pipeline.model_validate_yaml(fp)

            # Inject bundle variables. Pipeline-level variables take priority
            # because inject_vars() applies them on top of the provided vars dict.
            pl = pl.inject_vars(vars=bundle_vars)

            orchestrator = pl.orchestrator
            if not orchestrator:
                logger.info(f"Pipeline '{pl.name}' has no orchestrator. Skipping.")
                continue

            # Write pipeline config JSON for DABs to sync to the workspace
            config_file = getattr(orchestrator, "config_file", None)
            if config_file:
                config_file.build()

            # to_dab_resource() returns the dab resource, and also copies supporting
            # files (e.g. DLT notebook) to build_root and sets notebook paths.
            dab_resource = orchestrator.to_dab_resource()
            resources.add_resource(orchestrator.resource_name, dab_resource)
            logger.info(f"Added DABs resource '{orchestrator.resource_name}'")

    return resources
