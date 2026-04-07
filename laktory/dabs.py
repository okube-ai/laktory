from pathlib import Path

from laktory._logger import get_logger

logger = get_logger(__name__)


def load_resources(bundle):
    """
    DABs Python entry point for loading Laktory pipeline resources.

    This function is called by the Databricks CLI during bundle resolution
    and returns Job and DLT Pipeline resources derived from the Laktory stack.
    It also writes pipeline config JSON files to ``settings.laktory_build_root``
    for DABs to sync to the workspace.

    To use, declare in ``databricks.yml``:

    .. code-block:: yaml

        variables:
          laktory_stack_filepath: ./stack.yml

        sync:
          include:
            - .laktory/pipelines/**

        python:
          venv_path: .venv
          resources:
            - 'laktory.dabs:load_resources'

    Parameters
    ----------
    bundle:
        DABs Bundle object provided by the Databricks CLI.

    Returns
    -------
    :
        DABs Resources object containing all pipeline/job definitions.
    """
    from databricks.bundles.core import Location
    from databricks.bundles.core import Resources

    from laktory.models.pipeline.pipeline import Pipeline
    from laktory.models.stacks.stack import Stack

    # Resolve stack filepath from bundle variable
    # stack_filepath = bundle.resolve_variable("laktory_stack_filepath")  # TODO: Review why this method does not work and returns "laktory_stack_filepath"
    stack_filepath = bundle.variables["laktory_stack_filepath"]
    logger.info(f"Loading Laktory stack from '{stack_filepath}'")

    with open(stack_filepath, "r", encoding="utf-8") as fp:
        stack = Stack.model_validate_yaml(fp)

    env = stack.get_env(env_name=None)
    env = env.inject_vars()

    resources = Resources()

    for k, r in env.resources._get_all(providers_excluded=True).items():
        if not isinstance(r, Pipeline):
            continue
        orchestrator = r.orchestrator
        if not orchestrator:
            continue

        # Write pipeline config JSON for DABs to sync to the workspace
        config_file = getattr(orchestrator, "config_file", None)
        if config_file:
            config_file.build()

        # Return orchestrator as a DABs resource object.
        # We pass an explicit location pointing to the generated pipeline YAML
        # in laktory_build_root/pipelines/ so that DABs resolves relative paths
        # (e.g. notebook paths like ./dlt_laktory_pl.py) relative to that
        # directory instead of the laktory package source directory.
        from laktory._settings import settings

        build_yaml = (
            Path(settings.laktory_build_root) / "pipelines" / (r.name + ".yml")
        ).resolve()
        location = Location(file=str(build_yaml))

        dab_resource = orchestrator.to_dab_resource()
        resources.add_resource(
            orchestrator.resource_name, dab_resource, location=location
        )
        logger.info(f"Added DABs resource '{orchestrator.resource_name}'")

    return resources
