from typing import Optional
from pydantic import Field
from pydantic import field_validator
from pydantic_core.core_schema import FieldValidationInfo

from laktory._settings import settings
from laktory.models.base import BaseModel
from laktory.models.producer import Producer


class EventHeader(BaseModel):
    name: str
    description: str | None = None
    producer: Producer = None
    landing_mount_path: str = settings.landing_mount_path
    dirpath: Optional[str] = Field(validate_default=True, default=None)

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @field_validator("dirpath")
    def default_dirpath(cls, v: str, info: FieldValidationInfo) -> str:
        if v is None:
            data = info.data
            producer = ""
            if data["producer"] is not None:
                producer = data["producer"].name + "/"
            v = f'{data["landing_mount_path"]}events/{producer}{data["name"]}/'
        return v
