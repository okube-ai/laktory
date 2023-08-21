from typing import Literal

from laktory.models.sources.basesource import BaseSource


class TableSource(BaseSource):
    name: str | None
