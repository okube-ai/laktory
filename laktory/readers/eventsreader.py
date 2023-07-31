from pyspark.sql.dataframe import DataFrame
from pydantic import field_validator
from pydantic_core.core_schema import FieldValidationInfo

from typing_extensions import Annotated

from laktory.readers.basereader import BaseReader
from laktory._settings import settings

from pydantic import Field, field_validator
from typing import Optional


class EventsReader(BaseReader):
    events_root_path: str = settings.landing_mount_path + "events"
    producer_name: str = None
    event_name: str = None
    load_path: Optional[str] = Field(validate_default=True, default=None)

    @field_validator("load_path")
    def default_load_path(cls, v: str, info: FieldValidationInfo) -> str:
        if v is None:
            data = info.data
            v = f'{data["events_root_path"]}/{data["producer_name"]}/{data["event_name"]}'
        return v

    def read(self, spark) -> DataFrame:

        return (
            spark
            .read
            .option("multiLine", self.multiline)
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .format(self.fmt)
            .load(self.load_path)
        )

