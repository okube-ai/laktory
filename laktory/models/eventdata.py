import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

from laktory.models.eventdefinition import EventDefinition


class EventData(EventDefinition):
    data: dict

    def model_post_init(self, __context):
        # Add metadata
        self.data["_name"] = self.name
        self.data["_producer_name"] = self.producer.name
        tstamp = self.data.get("created_at", datetime.utcnow())
        if not tstamp.tzinfo:
            tstamp = tstamp.replace(tzinfo=ZoneInfo("utc"))
        self.data["_created_at"] = tstamp

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def created_at(self) -> datetime:
        return self.data["_created_at"]

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def landing_dirpath(self) -> str:
        t = self.created_at
        return f"{super().landing_dirpath}{t.year:04d}/{t.month:02d}/{t.day:02d}"

    def get_landing_filename(self, fmt="json", suffix=None) -> str:
        t = self.created_at
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

    # ----------------------------------------------------------------------- #
    # Output                                                                  #
    # ----------------------------------------------------------------------- #

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        exclude = kwargs.pop(
            "exclude",
            [
                "ingestion_pattern",
                "tstamp_col",
                "landing_mount_path",
            ]
        )
        mode = kwargs.pop("mode", "json")
        return super().model_dump(*args, exclude=exclude, mode=mode, **kwargs)

    def model_dump_json(self, *args, **kwargs) -> str:
        exclude = kwargs.pop(
            "exclude",
            [
                "ingestion_pattern",
                "tstamp_col",
                "landing_mount_path",
            ]
        )
        return super().model_dump_json(*args, exclude=exclude, **kwargs)
