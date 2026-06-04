from laktory._logger import get_logger

logger = get_logger(__name__)


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


def _post_execute():
    """Update pipeline tables metadata as a script"""
    # TODO: Refactor and integrate into dispatcher / executor / CLI

    import argparse

    import laktory as lk

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Read pipeline configuration file and apply tables configuration."
    )
    parser.add_argument(
        "--filepaths", type=str, help="Pipeline configuration filepaths", required=True
    )
    parser.add_argument(
        "--tables_metadata",
        type=str2bool,
        help="Update tables metadata",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--data_profiling_configs",
        type=str2bool,
        help="Update Databricks data profiling configs",
        default=False,
        required=False,
    )

    # Get arguments
    args, unknown = parser.parse_known_args()
    filepaths = args.filepaths.split(",")
    tables_metadata = args.tables_metadata
    data_profiling_configs = args.data_profiling_configs
    logger.info(f"Executing metadata update for pipelines {filepaths}")

    # Read
    for filepath in filepaths:
        with open(filepath, "r") as fp:
            if str(filepath).endswith(".yaml"):
                pl = lk.models.Pipeline.model_validate_yaml(fp)
            else:
                pl = lk.models.Pipeline.model_validate_json(fp.read())

        # Execute
        if tables_metadata:
            pl.update_tables_metadata()
        if data_profiling_configs:
            pl.update_data_profiling_configs()
