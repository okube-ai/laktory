from laktory._logger import get_logger

logger = get_logger(__name__)


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
        "--selects",
        type=str,
        help="Nodes selection",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--refresh",
        type=str,
        help=(
            "What the run does: incremental (no reset, sinks written according to their "
            "mode), full (reset, then reprocess all the data) or reset (reset only)"
        ),
        default="incremental",
        required=False,
    )
    parser.add_argument(
        "--reset_mode",
        type=str,
        help="Override of sinks reset mode (DROP or TRUNCATE)",
        default=None,
        required=False,
    )

    # Get arguments
    args, unknown = parser.parse_known_args()
    filepath = args.filepath
    selects = args.selects
    refresh = args.refresh or "incremental"
    reset_mode = args.reset_mode or None
    selects_str = ""
    if selects:
        selects = selects.split(",")
        selects_str = f" nodes {selects} from"
    logger.info(
        f"Executing{selects_str} pipeline '{filepath}' with refresh '{refresh}'"
    )

    # Read
    with open(filepath, "r", encoding="utf-8-sig") as fp:
        if str(filepath).endswith(".yaml"):
            pl = lk.models.Pipeline.model_validate_yaml(fp)
        else:
            pl = lk.models.Pipeline.model_validate_json(fp.read())

    # Execute
    pl.execute(
        selects=selects,
        refresh=refresh,
        reset_mode=reset_mode,
    )
