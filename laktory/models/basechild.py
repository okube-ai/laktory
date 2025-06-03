from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from laktory.models.basemodel import ModelMetaclass


# BaseChild must share the same metaclass as BaseModel for intellisense to work
# properly.
class BaseChild(BaseModel, metaclass=ModelMetaclass):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )
    _parent: Any = None

    @model_validator(mode="after")
    def assign_parent_to_children(self) -> Any:
        self._assign_parent_to_children()
        return self

    @property
    def children_names(self) -> list[str]:
        return []

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        self._parent = value

    def update_from_parent(self):
        pass

    # def _assign_parent_to_children(self):
    #     for cname in self.children_names:
    #         child = getattr(self, cname)
    #         if child is not None:
    #             child._parent = self
    #             child.update_from_parent()
    #             # child._assign_parent_to_children()

    def model_copy(self, *args, **kwargs):
        model = super().model_copy(*args, **kwargs)
        model._assign_parent_to_children()
        return model

    def _assign_parent_to_children(self):
        def _set_parent(o, parent=self):
            if isinstance(o, BaseChild):
                o._parent = parent
                o.update_from_parent()

        for c_name in self.children_names:
            o = getattr(self, c_name)
            if isinstance(o, list):
                for _o in o:
                    _set_parent(_o)
            elif isinstance(o, dict):
                for _o in o.values():
                    _set_parent(_o)
            else:
                _set_parent(o)
