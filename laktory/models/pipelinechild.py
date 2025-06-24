from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.enums import DataFrameBackends
from laktory.models.basechild import BaseChild

logger = get_logger(__name__)


class PipelineChild(BaseChild):
    """
    Pipeline Child Class
    """

    # model_config = ConfigDict(validate_assignment=True)
    dataframe_backend: DataFrameBackends = Field(
        None, description="Type of DataFrame backend"
    )
    dataframe_api: Literal["NARWHALS", "NATIVE"] = Field(
        None,
        description="""
        DataFrame API to use in DataFrame Transformer nodes. Either 'NATIVE' (backend-specific) or 'NARWHALS' 
        (backend-agnostic).
        """,
    )

    @property
    def df_backend(self) -> DataFrameBackends:
        # Direct value
        backend = self.dataframe_backend
        if backend is not None:
            if not isinstance(backend, DataFrameBackends):
                try:
                    backend = DataFrameBackends(backend)
                except ValueError:
                    pass
            return backend

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.df_backend

        # Value from settings
        return DataFrameBackends(settings.dataframe_backend.upper())

    @property
    def df_api(self) -> str:
        # Direct value
        dataframe_api = self.dataframe_api
        if dataframe_api is not None:
            return dataframe_api

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.df_api

        # Value from settings
        return settings.dataframe_api.upper()

    @property
    def parent_pipeline(self):
        from laktory.models.pipeline.pipeline import Pipeline

        def _get_pl(o):
            parent = getattr(o, "_parent", None)
            if parent is None:
                return None

            if isinstance(parent, Pipeline):
                return parent

            return _get_pl(parent)

        return _get_pl(self)

    @property
    def parent_pipeline_node(self):
        from laktory.models.pipeline.pipelinenode import PipelineNode

        def _get_pl(o):
            parent = getattr(o, "_parent", None)
            if parent is None:
                return None

            if isinstance(parent, PipelineNode):
                return parent

            return _get_pl(parent)

        return _get_pl(self)
