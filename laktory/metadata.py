from typing import Union


from laktory.models.databricks.pipeline import Pipeline
from laktory._settings import settings


def read_metadata(pipeline: str = None) -> Union[Pipeline]:
    if pipeline is not None:
        filepath = (
            f"/Workspace{settings.workspace_laktory_root}pipelines/{pipeline}.json"
        )
        with open(filepath, "r") as fp:
            pl = Pipeline.model_validate_json(fp.read())

        return pl
