from typing import Optional
from typing import Union
from pydantic import Field
from pydantic import field_validator
from pydantic_core.core_schema import FieldValidationInfo

from laktory._settings import settings
from laktory.models.base import BaseModel
from laktory.models.producer import Producer


class DataEventHeader(BaseModel):
    name: str = Field(...)
    description: Union[str, None] = Field(None)
    producer: Producer = Field(None)
    events_root: str = settings.workspace_landing_root + "events/"
    # event_root: Optional[str] = Field(validate_default=True, default=None)

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def event_root(self):
        producer = ""
        if self.producer is not None:
            producer = self.producer.name + "/"
        v = f'{self.events_root}{producer}{self.name}/'
        return v

    #
    # @field_validator("root_path")
    # def default_root_path(cls, v: str, info: FieldValidationInfo) -> str:
    #     if v is None:
    #         data = info.data
    #         producer = ""
    #         if data.get("producer") is not None:
    #             producer = data["producer"].name + "/"
    #         v = f'{data.get("events_root_path", "")}{producer}{data.get("name", "")}/'
    #     return v
