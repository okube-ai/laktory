from collections.abc import Callable
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import Union
from typing import get_args
from typing import get_origin

import narwhals as nw
from pydantic import Discriminator
from pydantic import Tag
from pydantic_core import CoreSchema
from pydantic_core import core_schema

if TYPE_CHECKING:
    pass

AnyFrame = nw.LazyFrame | nw.DataFrame


class VariableType(str):
    """
    A string placeholder that defers field resolution to `inject_vars()`.

    Any model field accepting `VariableType` in addition to its declared type,
    allows values to be specified as variable references or expressions in
    YAML configs and resolved at deploy time.

    The string **must** start with `${` - arbitrary strings are not accepted.
    Two syntaxes are supported:

    - Simple substitution: `${vars.my_variable_name}`
    - Python expression:   `${{ 4 if vars.env == 'prod' else 1 }}`

    Examples
    --------
    ```yaml
    num_workers: ${vars.workers}
    size: ${{ 4 if vars.env == 'prod' else 1 }}
    access_controls: ${vars.default_access_controls}
    ```

    References
    ----------
    * [variables](https://www.laktory.ai/concepts/variables/)
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type, handler: callable
    ) -> CoreSchema:
        return core_schema.str_schema(pattern=r"^\$\{")


def tagged_union(
    members: list[type],
    infer: Callable[[dict], type | None],
    kind: str,
    nullable: bool = False,
) -> Any:
    """
    Build a Pydantic discriminated union of `members` and `VariableType`.

    The member is selected before validation, so a validation error is reported
    against that member only (one error, short path) instead of against every
    member of the union. Selection rules:

    - `str`: `VariableType`
    - model instance: its class (`isinstance`, so subclasses are accepted)
    - dict with a `type`: the member whose `type` field defaults to that value
      (a `${...}` variable is an error: the member must be known when parsing)
    - other dict: `infer(data)`, which returns the member class from the keys

    Parameters
    ----------
    members:
        Union member classes. Their names are used as tags (and in error paths).
    infer:
        Selects the member from the keys of a dict without an explicit `type`.
        Frozen to the members existing when inference was introduced: new
        members must be selected through their `type`.
    kind:
        Name of the union used in error messages (e.g. "sink").
    nullable:
        Accept `None` (e.g. a field defaulting to `None`).

    Returns
    -------
    :
        Annotated union type
    """
    types = {}
    for cls in members:
        f = cls.model_fields.get("type")
        if f is not None and isinstance(f.default, str):
            types[f.default] = cls

    def _tag(data: Any) -> str | None:
        if data is None and nullable:
            return "None"
        if isinstance(data, str):
            return VariableType.__name__
        if isinstance(data, dict):
            t = data.get("type")
            if isinstance(t, str):
                cls = types.get(t)
                return cls.__name__ if cls else t
            cls = infer(data)
            return cls.__name__ if cls else None
        for cls in members:
            if isinstance(data, cls):
                return cls.__name__
        return None

    expected = ", ".join(f"'{t}'" for t in sorted(types))
    tagged = tuple(Annotated[cls, Tag(cls.__name__)] for cls in members)
    tagged += (Annotated[VariableType, Tag(VariableType.__name__)],)
    if nullable:
        tagged += (Annotated[None, Tag("None")],)
    return Annotated[
        Union[tagged],  # noqa: UP007 (members built dynamically)
        Discriminator(
            _tag,
            custom_error_type=f"invalid_{kind.replace(' ', '_')}_type",
            custom_error_message=f"Unable to determine the {kind} type. Set `type` to one of: {expected}.",
        ),
    ]


def _is_placeholder(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("${")


def _passthrough_placeholder(value: Any, handler: Callable) -> Any:
    # Wrap validator and serializer: a `${...}` placeholder is kept as-is
    if _is_placeholder(value):
        return value
    return handler(value)


class _VariableOr:
    """
    `Annotated` marker for `Union[T, VariableType]`: a `${...}` placeholder is kept
    as-is, any other value is validated against `T` only. Same result as the
    plain union, but a validation error is reported once, against `T`, with a
    short path (no `function-after[...]` / `constrained-str` members).
    """

    def __get_pydantic_core_schema__(self, source: Any, handler: Any) -> CoreSchema:
        args = tuple(a for a in get_args(source) if a is not VariableType)
        inner_type = args[0] if len(args) == 1 else Union[args]  # noqa: UP007
        inner = handler.generate_schema(inner_type)
        return core_schema.no_info_wrap_validator_function(
            _passthrough_placeholder,
            inner,
            json_schema_input_schema=core_schema.union_schema(
                [inner, handler.generate_schema(VariableType)]
            ),
            serialization=core_schema.wrap_serializer_function_ser_schema(
                _passthrough_placeholder, schema=inner
            ),
        )

    def __repr__(self) -> str:
        return "VariableOr"


VARIABLE_OR = _VariableOr()


def variable_or(type_: Any) -> Any:
    """`type_ | VariableType`, validated by `_VariableOr`."""
    return Annotated[Union[type_, VariableType], VARIABLE_OR]  # noqa: UP007


def unwrap_variable_or(annotation: Any) -> Any:
    """Return `T` from `variable_or(T)` (or from `T | VariableType`)."""
    if get_origin(annotation) is Annotated and VARIABLE_OR in get_args(annotation):
        annotation = get_args(annotation)[0]
    if get_origin(annotation) is Union:
        args = tuple(a for a in get_args(annotation) if a is not VariableType)
        if len(args) == 1:
            return args[0]
        return Union[args]  # noqa: UP007
    return annotation


def is_tagged_union(annotation: Any) -> bool:
    """Whether `annotation` was built with `tagged_union()`."""
    if get_origin(annotation) is not Annotated:
        return False
    return any(isinstance(m, Discriminator) for m in get_args(annotation)[1:])
