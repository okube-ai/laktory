from typing import Union


from laktory.models.databricks.dltpipeline import DLTPipeline
from laktory._settings import settings


def read_metadata(pipeline: str = None) -> Union[DLTPipeline]:
    if pipeline is not None:
        filepath = (
            f"/Workspace{settings.workspace_laktory_root}pipelines/{pipeline}.json"
        )
        with open(filepath, "r") as fp:
            pl = DLTPipeline.model_validate_json(fp.read())

        return pl
