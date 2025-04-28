from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.enums import DataFrameBackends

logger = get_logger(__name__)


class PipelineChild(BaseModel):
    """
    Pipeline Child Class
    """

    model_config = ConfigDict(validate_assignment=False)
    dataframe_backend: DataFrameBackends = Field(
        None, description="Type of DataFrame backend"
    )
    dataframe_api: Literal["NARWHALS", "NATIVE"] = Field(
        None,
        description="DataFrame API to use in DataFrame Transformer nodes. Either 'NATIVE' (backend-specific) or 'NARWHALS' (backend-agnostic)."
    )

    @model_validator(mode="after")
    def update_children_after_init(self) -> Any:
        if not hasattr(self, "_parent"):
            self._parent = None
        self.update_children()
        return self

    def model_copy(self, *args, **kwargs):
        model = super().model_copy(*args, **kwargs)
        model.update_children()
        return model

    @property
    def child_attribute_names(self):
        return []

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        self._parent = value
        self.update_children()

    @property
    def df_backend(self) -> DataFrameBackends:
        # Direct value
        backend = self.dataframe_backend
        if backend is not None:
            return backend

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.df_backend

        # Value from settings
        return DataFrameBackends(settings.dataframe_backend.upper())

    @property
    def _dataframe_api(self) -> str:

        # Direct value
        dataframe_api = self.dataframe_api
        if dataframe_api is not None:
            return dataframe_api

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent._dataframe_api

        # Value from settings
        return settings.dataframe_api.upper()

    def update_children(self):
        for c_name in self.child_attribute_names:
            o = getattr(self, c_name)
            if o is None:
                continue
            elif isinstance(o, list):
                for _o in o:
                    _o.parent = self
            elif isinstance(o, dict):
                for _o in o.values():
                    _o.parent = self
            else:
                o.parent = self

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
