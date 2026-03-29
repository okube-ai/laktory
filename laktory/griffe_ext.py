"""Custom Griffe extension: sort Pydantic fields and hide infrastructure base class members."""

from __future__ import annotations

from typing import Any

from griffe import Attribute
from griffe import Class
from griffe import DocstringSectionAttributes
from griffe import DocstringSectionParameters
from griffe import Extension
from griffe import dynamic_import
from griffe import get_logger

logger = get_logger("laktory.griffe_ext")


class FieldSorterExtension(Extension):
    """Sort Pydantic fields alphabetically and hide infrastructure base class members.

    Base classes marked with ``__doc_hide_base__ = True`` have ALL their fields
    and public methods hidden in child class documentation. All remaining fields
    (own + domain-parent) are merged into one sorted "PARAMETER" section.

    Individual child classes can additionally hide specific inherited fields::

        class MyModel(ParentModel):
            __hidden_doc_fields__: ClassVar[set[str]] = {"some_field"}
    """

    def on_class(self, *, cls: Class, **kwargs: Any) -> None:
        if not cls.docstring:
            return

        try:
            runtime_obj = dynamic_import(cls.path)
        except ImportError:
            logger.debug("Could not import %s", cls.path)
            return

        # Identify which ancestor classes are "hidden bases"
        hidden_base_names: set[str] = {
            ancestor.__name__
            for ancestor in runtime_obj.__mro__[1:]
            if ancestor.__dict__.get("__doc_hide_base__", False)
        }

        # Collect field names owned by hidden base classes
        hidden_field_names: set[str] = set()
        for ancestor in runtime_obj.__mro__[1:]:
            if ancestor.__name__ in hidden_base_names:
                hidden_field_names |= set(
                    ancestor.__dict__.get("__annotations__", {}).keys()
                )

        # Collect per-class explicitly hidden fields (__hidden_doc_fields__ accumulates across MRO)
        per_class_hidden: set[str] = set()
        for ancestor in runtime_obj.__mro__:
            per_class_hidden |= set(
                ancestor.__dict__.get("__hidden_doc_fields__", set())
            )

        all_hidden_fields = hidden_field_names | per_class_hidden

        # Shadow public members of hidden griffe base classes with no-docstring stubs.
        # Because inherited_members only includes names NOT in cls.members, the stub
        # prevents the real inherited member from appearing. show_if_no_docstring=false
        # then hides the stub itself.
        if hidden_base_names:
            try:
                for base_griffe_cls in cls.mro():
                    if base_griffe_cls.name in hidden_base_names:
                        for member_name in list(base_griffe_cls.members.keys()):
                            if (
                                not member_name.startswith("_")
                                and member_name not in cls.members
                            ):
                                cls.members[member_name] = Attribute(
                                    member_name, parent=cls
                                )
            except (ValueError, Exception) as e:
                logger.debug("MRO walk failed for %s: %s", cls.path, e)

        # Filter hidden fields and sort all remaining fields alphabetically
        sections = cls.docstring.parsed
        for section in list(sections):
            if not isinstance(
                section, (DocstringSectionParameters, DocstringSectionAttributes)
            ):
                continue
            visible = [
                item for item in section.value if item.name not in all_hidden_fields
            ]
            visible.sort(key=lambda x: x.name)
            section.value = visible

        # Remove empty parameter/attribute sections
        cls.docstring.parsed = [
            s
            for s in sections
            if not isinstance(
                s, (DocstringSectionParameters, DocstringSectionAttributes)
            )
            or s.value
        ]
