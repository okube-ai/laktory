from laktory._logger import get_logger

logger = get_logger(__name__)


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


def _execute():
    """Execute pipeline as a script"""
    # TODO: Refactor and integrate into dispatcher / executor / CLI

    import argparse

    import laktory as lk

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Read pipeline configuration file and execute"
    )
    parser.add_argument(
        "--filepath", type=str, help="Pipeline configuration filepath", required=True
    )
    parser.add_argument(
        "--node_name",
        type=str,
        help="Node name",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--full_refresh",
        type=str2bool,
        help="Full refresh",
        default=False,
        required=False,
    )

    # Get arguments
    args, unknown = parser.parse_known_args()
    filepath = args.filepath
    node_name = args.node_name
    full_refresh = args.full_refresh
    node_str = ""
    if node_name:
        node_str = f" node '{node_name}' of "
    logger.info(
        f"Executing{node_str} pipeline '{filepath}' with full refresh {full_refresh}"
    )

    # Read
    with open(filepath, "r") as fp:
        if str(filepath).endswith(".yaml"):
            pl = lk.models.Pipeline.model_validate_yaml(fp)
        else:
            pl = lk.models.Pipeline.model_validate_json(fp.read())

    # Execute
    if node_name:
        pl.nodes_dict[node_name].execute(full_refresh=full_refresh)
    else:
        pl.execute(full_refresh=full_refresh)
