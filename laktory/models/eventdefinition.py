import os

from laktory.models.base import BaseModel
from laktory.models.producer import Producer
from laktory.models.ingestionpattern import IngestionPattern
from laktory._settings import settings


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #

class EventDefinition(BaseModel):
    name: str
    description: str = None
    producer: Producer = None
    ingestion_pattern: IngestionPattern = IngestionPattern()
    tstamp_col: str = "created_at"
    landing_mount_path: str = settings.landing_mount_path

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def landing_dirpath(self):
        return f"{self.landing_mount_path}events/{self.producer.name}/{self.name}/"
