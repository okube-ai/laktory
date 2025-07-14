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
