from typing import Literal

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.enums import DataFrameBackends
from laktory.models.basechild import BaseChild

logger = get_logger(__name__)


class PipelineChild(BaseChild):
    """
    Pipeline Child Class
    """

    __doc_hide_base__ = True  # hide this class's fields and methods from child docs

    # model_config = ConfigDict(validate_assignment=True)
    dataframe_backend_: DataFrameBackends = Field(
        None,
        description="Type of DataFrame backend",
        validation_alias=AliasChoices("dataframe_backend", "dataframe_backend_"),
        exclude=True,
    )
    dataframe_api_: Literal["NARWHALS", "NATIVE"] = Field(
        None,
        description="""
        DataFrame API to use in DataFrame Transformer nodes. Either 'NATIVE' (backend-specific) or 'NARWHALS' 
        (backend-agnostic).
        """,
        validation_alias=AliasChoices("dataframe_api", "dataframe_api_"),
        exclude=True,
    )

    @computed_field(description="dataframe_backend")
    @property
    def dataframe_backend(self) -> DataFrameBackends:
        backend = self.dataframe_backend_

        # Direct value
        if backend is not None:
            if not isinstance(backend, DataFrameBackends):
                try:
                    backend = DataFrameBackends(backend)
                except ValueError:
                    # TODO: Review why this might occur
                    pass

            return backend

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.dataframe_backend

        # Value from settings
        return DataFrameBackends(settings.dataframe_backend.upper())

    @computed_field(description="dataframe_api")
    @property
    def dataframe_api(self) -> str:
        # Direct value
        if self.dataframe_api_:
            return self.dataframe_api_

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.dataframe_api

        # Value from settings
        return settings.dataframe_api.upper()

    full_refresh_mode_: Literal["DROP", "TRUNCATE"] = Field(
        None,
        description="""
        Strategy used to purge a sink's data when `full_refresh` is requested.

        - DROP: Drop the table (or delete the file/data) entirely, then recreate it on next write.
        - TRUNCATE: Remove all rows but keep the table/schema/location intact.

        `DELETE_WHERE` is also available, but only directly on a data sink (see
        `BaseDataSink.full_refresh_mode`) - a deletion predicate is inherently specific to a single
        sink, so it can't be a pipeline node, pipeline, or global default.
        """,
        validation_alias=AliasChoices("full_refresh_mode", "full_refresh_mode_"),
        exclude=True,
    )

    def _resolve_full_refresh_mode(self) -> str:
        # Direct value
        if self.full_refresh_mode_ is not None:
            return self.full_refresh_mode_

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.full_refresh_mode

        # Value from settings
        return settings.full_refresh_mode.upper()

    @computed_field(description="full_refresh_mode")
    @property
    def full_refresh_mode(self) -> Literal["DROP", "TRUNCATE"]:
        return self._resolve_full_refresh_mode()

    @property
    def parent_pipeline(self):
        from laktory.models.pipeline.pipeline import Pipeline

        node = self
        for _ in range(50):
            parent = getattr(node, "_parent", None)
            if parent is None:
                return None
            if isinstance(parent, Pipeline):
                return parent
            node = parent
        return None

    @property
    def parent_pipeline_node(self):
        from laktory.models.pipeline.pipelinenode import PipelineNode

        node = self
        for _ in range(50):
            parent = getattr(node, "_parent", None)
            if parent is None:
                return None
            if isinstance(parent, PipelineNode):
                return parent
            node = parent
        return None
