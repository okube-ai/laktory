from laktory._logger import get_logger

logger = get_logger(__name__)

def _update_tables_metadata():
    """Update pipeline tables metadata as a script"""
    # TODO: Refactor and integrate into dispatcher / executor / CLI

    import argparse

    import laktory as lk

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Read pipeline configuration file and update tables metadata"
    )
    parser.add_argument(
        "--filepaths", type=str, help="Pipeline configuration filepaths", required=True
    )
    parser.add_argument(
        "--env",
        type=str,
        help="Dummy argument to facilitate databricks jobs",
        required=False,
    )

    # Get arguments
    args = parser.parse_args()
    filepaths = args.filepaths.split(",")
    logger.info(f"Executing metadata update for pipelines {filepaths}")

    # Read
    for filepath in filepaths:
        with open(filepath, "r") as fp:
            if str(filepath).endswith(".yaml"):
                pl = lk.models.Pipeline.model_validate_yaml(fp)
            else:
                pl = lk.models.Pipeline.model_validate_json(fp.read())

        # Execute
        pl.update_tables_metadata()
