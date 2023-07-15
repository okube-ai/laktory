import os

from laktory.models.base import BaseModel
from laktory.models.producer import Producer
from laktory.models.ingestionpattern import IngestionPattern


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #

class EventDefinition(BaseModel):
    name: str
    description: str = None
    producer: Producer = None
    ingestion_pattern: IngestionPattern = IngestionPattern()
    tstamp_col: str = "created"
    landing_mount_path: str = os.getenv("LANDING_MOUNT_PATH", "mnt/landing/")  # TODO: Replace with setting

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def landing_dirpath(self):
        return f"{self.landing_mount_path}/events/{self.producer.name}/{self.name}/"
