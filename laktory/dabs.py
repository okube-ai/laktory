import os
from pathlib import Path

from laktory._logger import get_logger
from laktory._settings import DEFAULT_LAKTORY_BUILD_ROOT
from laktory._settings import DEFAULT_LAKTORY_ROOT

logger = get_logger(__name__)


def load_resources(bundle):
    """
    DABs Python entry point for loading Laktory pipeline resources.

    This function is called by the Databricks CLI during bundle resolution
    and returns Job and DLT Pipeline resources derived from the Laktory stack.
    It also writes pipeline config JSON files for DABs to sync to the workspace.

    Two global settings are configured automatically when not already set by
    the stack:

    - ``settings.laktory_build_root`` defaults to ``laktory/.build/`` relative
      to the bundle root (the directory containing ``databricks.yml``).
    - ``settings.workspace_laktory_root`` is derived from the
      ``dab_workspace_root`` bundle variable (if provided) as
      ``{dab_workspace_root}/files/{laktory_build_root}/``.

    To expose the workspace root path, add to ``databricks.yml``:

    .. code-block:: yaml

        variables:
          dab_workspace_root:
            default: ${workspace.root_path}

    To use, declare in ``databricks.yml``:

    .. code-block:: yaml

        variables:
          laktory_stack_filepath: ./stack.yml
          dab_workspace_root:
            default: ${workspace.root_path}

        sync:
          paths:
            - ./laktory

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
    from databricks.bundles.core import Resources

    from laktory._settings import settings
    from laktory.models.pipeline.pipeline import Pipeline
    from laktory.models.stacks.stack import Stack

    # Resolve stack filepath from bundle variable
    stack_filepath = bundle.variables["laktory_stack_filepath"]
    logger.info(f"Loading Laktory stack from '{stack_filepath}'")

    with open(stack_filepath, "r", encoding="utf-8") as fp:
        stack = Stack.model_validate_yaml(fp)

    # target = bundle.target
    env = stack.get_env(env_name=None)

    # Get Bundle (databricks.yml) directory. This only works if CLI is called from the
    # same directory (i.e. --bundle-dir is not used)
    # TODO: build a more reliable approach.
    bundle_dirpath = Path(os.getcwd())

    # Laktory Build Root
    if settings.laktory_build_root == DEFAULT_LAKTORY_BUILD_ROOT:
        settings.laktory_build_root = str(bundle_dirpath / "laktory" / ".build")
    logger.info(
        f"Setting `laktory_build_root` to default '{settings.laktory_build_root}'. Make sure this path is added to Bundle sync paths."
    )

    # Workspace Laktory root
    # This is where Laktory files (pipeline config, queries, dashboards, etc.) are
    # deployed. When using Laktory only, default is /Workspace/.laktory/. In
    # the context of DAB, we set it to {dab_workspace_root}/laktory/.build/
    # Unfortuantely, {dab_workspace_root} is not available unless the user
    # adds it to the variables.
    dab_workspace_root = bundle.variables.get("dab_workspace_root")
    if settings.workspace_laktory_root == DEFAULT_LAKTORY_ROOT:
        if dab_workspace_root is None:
            raise ValueError(
                "Variale `dab_workspace_root` must be set to '${workspace.root_path}' in databricks.yml to use Laktory."
            )

        # Build Path relative to Bundle root
        build_root_abs = settings.laktory_build_root
        build_root_rel = os.path.relpath(build_root_abs, bundle_dirpath)
        settings.workspace_laktory_root = (
            f"{dab_workspace_root}/files/{build_root_rel}/"
        )

    # Laktory expect the workspace root to exclude "/Workspace/"
    settings.workspace_laktory_root = settings.workspace_laktory_root.replace(
        "/Workspace/", "/"
    )

    # --- Inject variables ---
    # Expose bundle variables to the stack. Laktory variables (declared in the
    # stack or its resources) take priority because inject_vars() applies them
    # on top of the provided vars dict via vars.update(self.variables).
    bundle_vars = {
        k: v
        for k, v in bundle.variables.items()
        if v is not None and k not in env.variables
    }
    env = env.inject_vars(vars=bundle_vars)

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

        # to_dab_resource() returns the dab resource, but also copies supporting
        # files (e.g. DLT notebook) to laktory_build_root and sets notebook paths.
        dab_resource = orchestrator.to_dab_resource()
        resources.add_resource(orchestrator.resource_name, dab_resource)
        logger.info(f"Added DABs resource '{orchestrator.resource_name}'")

    return resources
