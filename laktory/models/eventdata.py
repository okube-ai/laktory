import os
from datetime import datetime
from laktory.models.base import BaseModel
from laktory.models.eventdefinition import EventDefinition


class EventData(BaseModel):
    name: str
    data: dict
    producer_name: str

    def model_post_init(self, __context):
        # Add metadata
        self.data["_name"] = self.name
        self.data["_producer_name"] = self.producer_name
        self.data["_created"] = self.data.get("created", datetime.utcnow())

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def created(self) -> datetime:
        return self.data["_created"]

    @property
    def event_definition(self):
        return EventDefinition(
            name=self.name,
            producer={"name": self.producer_name}
        )

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def landing_dirpath(self) -> str:
        t = self.created
        return f"{self.event_definition.landing_dirpath}/{t.year:04d}/{t.month:02d}/{t.day:02d}"

    def get_landing_filename(self, fmt="json", suffix=None) -> str:
        t = self.created
        const = {"mus": {"s": 1e-6}, "s": {"ms": 1000}}  # TODO: replace with constants
        total_ms = int((t.second + t.microsecond * const["mus"]["s"]) * const["s"]["ms"])
        time_str = f"{t.hour:02d}{t.minute:02d}{total_ms:05d}Z"
        prefix = self.name
        if suffix is not None:
            prefix += f"_{suffix}"
        if fmt == "json_stream":
            fmt = "txt"
        return f"{prefix}_{t.year:04d}{t.month:02d}{t.day:02d}T{time_str}.{fmt}"

    def get_landing_filepath(self, fmt="json", suffix=None):
        return os.path.join(self.landing_dirpath, self.get_landing_filename(fmt, suffix))
