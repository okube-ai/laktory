from typing import Any
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class Runner(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = None
    id: str = None
    dispatcher: Any = Field(default=None, exclude=True)

    @property
    def wc(self):
        return self.dispatcher.wc

    def get_id(self):
        raise NotImplementedError()
