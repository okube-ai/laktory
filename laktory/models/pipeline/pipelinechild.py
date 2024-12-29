from typing import Any
from typing import Literal
from pydantic import model_validator
from pydantic import BaseModel

from laktory._settings import settings
from laktory._logger import get_logger

logger = get_logger(__name__)


class PipelineChild(BaseModel):
    """ """

    dataframe_backend: Literal["SPARK", "POLARS"] = None

    @model_validator(mode="after")
    def update_children_after_init(self) -> Any:
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
    def df_backend(self) -> str:

        # Direct value
        backend = self.dataframe_backend
        if backend is not None:
            return backend

        # Value from parent
        parent = self._parent
        if parent is not None:
            return parent.df_backend

        # Value from settings
        return settings.dataframe_backend

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
