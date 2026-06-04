from laktory._logger import get_logger

logger = get_logger(__name__)


def _update_data_profiling_configs():
    """Update Databricks data profiling configs as a script"""
    # TODO: Refactor and integrate into dispatcher / executor / CLI

    import argparse

    import laktory as lk

    parser = argparse.ArgumentParser(
        description="Read pipeline configuration file and apply data profiling configs."
    )
    parser.add_argument(
        "--filepaths", type=str, help="Pipeline configuration filepaths", required=True
    )

    args, unknown = parser.parse_known_args()
    filepaths = args.filepaths.split(",")
    logger.info(f"Updating data profiling configs for pipelines {filepaths}")

    for filepath in filepaths:
        with open(filepath, "r") as fp:
            if str(filepath).endswith(".yaml"):
                pl = lk.models.Pipeline.model_validate_yaml(fp)
            else:
                pl = lk.models.Pipeline.model_validate_json(fp.read())

        pl.update_data_profiling_configs()
