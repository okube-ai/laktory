"""Custom Griffe extension: sort Pydantic fields and hide infrastructure base class members."""

from __future__ import annotations

from typing import Any

from griffe import Attribute
from griffe import Class
from griffe import DocstringSectionAttributes
from griffe import DocstringSectionParameters
from griffe import Expr
from griffe import ExprBinOp
from griffe import ExprName
from griffe import ExprSubscript
from griffe import Extension
from griffe import dynamic_import
from griffe import get_logger

logger = get_logger("laktory.griffe_ext")


def _replace_expr_name(
    expr: str | Expr | None,
    name: str,
    replacement: ExprName,
) -> str | Expr | None:
    """Recursively replace ExprName(name) nodes inside an expression tree."""
    if expr is None or isinstance(expr, str):
        return expr
    if isinstance(expr, ExprName):
        return replacement if expr.name == name else expr
    if isinstance(expr, ExprBinOp):
        expr.left = _replace_expr_name(expr.left, name, replacement)
        expr.right = _replace_expr_name(expr.right, name, replacement)
    elif isinstance(expr, ExprSubscript):
        expr.left = _replace_expr_name(expr.left, name, replacement)
        expr.slice = _replace_expr_name(expr.slice, name, replacement)
    return expr


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

        # Find the nearest generated base class (marked __doc_generated_base__ = True).
        # Its field names determine the "Base / Databricks" split vs "Laktory" split.
        generated_base = None
        generated_base_fields: set[str] = set()
        for ancestor in runtime_obj.__mro__[1:]:
            if ancestor.__dict__.get("__doc_generated_base__", False):
                generated_base = ancestor
                generated_base_fields = set(
                    ancestor.__dict__.get("__annotations__", {}).keys()
                )
                break

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

        # Filter hidden fields, split by origin (base vs Laktory), and sort.
        sections = cls.docstring.parsed
        new_sections: list = []
        for section in sections:
            if not isinstance(
                section, (DocstringSectionParameters, DocstringSectionAttributes)
            ):
                new_sections.append(section)
                continue

            visible = [
                item for item in section.value if item.name not in all_hidden_fields
            ]

            if generated_base is not None and isinstance(
                section, DocstringSectionParameters
            ):
                # Split: fields whose name appears in the generated base → "Base",
                # everything else → "Laktory".
                base_items = sorted(
                    [i for i in visible if i.name in generated_base_fields],
                    key=lambda x: x.name,
                )
                laktory_items = sorted(
                    [i for i in visible if i.name not in generated_base_fields],
                    key=lambda x: x.name,
                )
                if base_items:
                    new_sections.append(
                        DocstringSectionParameters(value=base_items, title="Base")
                    )
                if laktory_items:
                    new_sections.append(
                        DocstringSectionParameters(value=laktory_items, title="Laktory")
                    )
            else:
                visible.sort(key=lambda x: x.name)
                if visible:
                    section.value = visible
                    new_sections.append(section)

        cls.docstring.parsed = new_sections
        sections = new_sections

        # Qualify bare VariableType ExprName nodes so autorefs can link them.
        # griffe_fieldz uses display_as_type() which strips the module prefix,
        # producing ExprName('VariableType') with no parent context.  We fix
        # this by setting parent to the laktory.typing griffe module so that
        # canonical_path resolves to 'laktory.typing.VariableType'.
        try:
            typing_module = cls.package["typing"]
        except (KeyError, AttributeError):
            typing_module = None

        if typing_module is not None:
            qualified = ExprName("VariableType", parent=typing_module)
            for section in cls.docstring.parsed:
                if not isinstance(
                    section, (DocstringSectionParameters, DocstringSectionAttributes)
                ):
                    continue
                for item in section.value:
                    if item.annotation is not None:
                        item.annotation = _replace_expr_name(
                            item.annotation, "VariableType", qualified
                        )
