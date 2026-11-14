from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from laktory.models.basemodel import ModelMetaclass


# BaseChild must share the same metaclass as BaseModel for intellisense to work
# properly.
class BaseChild(BaseModel, metaclass=ModelMetaclass):
    __doc_hide_base__ = True  # hide this class's fields and methods from child docs

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )
    _parent: "BaseChild | None" = None

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

    def _iter_children(self):
        for c_name in self.children_names:
            o = getattr(self, c_name)
            if isinstance(o, list):
                children = o
            elif isinstance(o, dict):
                children = o.values()
            else:
                children = [o]
            for c in children:
                if isinstance(c, BaseChild):
                    yield c

    def _assign_parent_to_children(self):
        for c in self._iter_children():
            c._parent = self
            c.update_from_parent()
            # Recurse so `update_from_parent()` runs on the whole subtree, not
            # just direct children (`model_copy`'s override only fires
            # `_assign_parent_to_children()` on the copied object itself).
            c._assign_parent_to_children()

    def _copy_tree(self) -> "BaseChild":
        """
        Deep copy, with the `_parent` of each child pointing to its copied parent
        (see `BaseModel.__deepcopy__`). Unlike `model_copy(deep=True)`,
        `update_from_parent()` is not called: the copied children already hold
        the state derived from an identical parent (e.g. the tasks of a job
        orchestrator), which can be costly to rebuild.
        """
        return BaseModel.model_copy(self, deep=True)
