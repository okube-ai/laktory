import yaml
import json
from pydantic import BaseModel as _BaseModel
from pydantic import computed_field
from pydantic import ConfigDict

from laktory import settings


class BaseModel(_BaseModel):
    model_config = ConfigDict(extra="forbid")

    @property
    def workspace_client(self):
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token,
        )

    @classmethod
    def model_validate_yaml(cls, fp):
        data = yaml.safe_load(fp)
        return cls.model_validate(data)

    @classmethod
    def model_validate_json_file(cls, fp):
        data = json.load(fp)
        return cls.model_validate(data)
