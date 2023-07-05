import yaml
import json
from pydantic import BaseModel as _BaseModel


class BaseModel(_BaseModel):
    pass

    @classmethod
    def model_validate_yaml(cls, fp):
        data = yaml.safe_load(fp)
        return cls.model_validate(data)

    @classmethod
    def model_validate_json_file(cls, fp):
        data = json.load(fp)
        return cls.model_validate(data)
