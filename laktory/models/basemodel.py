#

import copy
import json
import re
import typing
from copy import deepcopy
from typing import Any
from typing import TextIO
from typing import Type
from typing import TypeVar
from typing import Union
from typing import get_args
from typing import get_origin

import pydantic
import yaml  # TODO: Move into functions?
from pydantic import AliasChoices
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import model_validator
from pydantic._internal._model_construction import ModelMetaclass as _ModelMetaclass

from laktory._parsers import _resolve_value
from laktory._parsers import _resolve_values
from laktory.typing import VariableType
from laktory.yaml.recursiveloader import RecursiveLoader

# ModelMetaclass relies on pydantic._internal internals; alert on untested upgrades.
_KNOWN_PYDANTIC_MINOR = (2, 13)
_pydantic_minor = tuple(int(x) for x in pydantic.__version__.split(".")[:2])
if _pydantic_minor != _KNOWN_PYDANTIC_MINOR:
    pass
    # warnings.warn(
    #     f"laktory uses pydantic._internal internals; tested against "
    #     f"{'.'.join(str(x) for x in _KNOWN_PYDANTIC_MINOR)}.x, "
    #     f"found {pydantic.__version__} - run the full test suite before deploying.",
    #     stacklevel=2,
    # )

Model = TypeVar("Model", bound="BaseModel")


class _PluralFieldSpec:
    """
    Defers plural alias creation until the field name is known by the metaclass.
    Use `PluralField()` instead of instantiating this directly.
    """

    __slots__ = ("field_info", "plural")

    def __init__(self, field_info, plural):
        self.field_info = field_info
        self.plural = plural  # None = auto-derive as field_name + "s"


def PluralField(default=None, *, plural: str = None, **kwargs):
    """
    Field helper for list fields whose name is the singular form (matching Terraform/DAB).
    The plural form is automatically added as a validation alias so YAML input can use
    either form. Set `plural` explicitly only for irregular plurals (e.g. `plural="libraries"`).
    """
    return _PluralFieldSpec(Field(default, **kwargs), plural)


def annotation_contains_list_of_basemodel(annotation, mymodel_cls) -> bool:
    origin = get_origin(annotation)
    args = get_args(annotation)

    # Base case: direct subclass
    if isinstance(annotation, type) and issubclass(annotation, mymodel_cls):
        return True

    # Handle ForwardRefs or strings (optional: depending on your context)
    if isinstance(annotation, str):
        return False  # or implement custom string resolution if needed

    # Handle generic containers like list, Union, dict, etc.
    if origin is not None:
        for arg in args:
            if annotation_contains_list_of_basemodel(arg, mymodel_cls):
                return True

    return False


# Fields always excluded from VariableType injection.
_VARTYPE_EXCLUDED_FIELDS: set[str] = {"variables", "dataframe_backend"}

# Per-class fields excluded from VariableType injection (e.g. discriminator fields
# that Pydantic uses to select a union variant - injecting VariableType breaks the
# discriminator logic at class-construction time).
_VARTYPE_EXCLUDED_CLASS_FIELDS: dict[str, set[str]] = {
    "UnityCatalogDataSink": {"type"},
    "HiveMetastoreDataSink": {"type"},
}


def _resolve_plural_fields(namespace: dict[str, Any]) -> None:
    for fname in list(namespace.keys()):
        value = namespace[fname]
        if isinstance(value, _PluralFieldSpec):
            plural = value.plural or (fname if fname.endswith("s") else (fname + "s"))
            fi = value.field_info
            alias = AliasChoices(plural)
            fi.validation_alias = alias
            fi._attributes_set["validation_alias"] = alias
            namespace[fname] = fi


def _expand_optional_fields(
    namespace: dict[str, Any], bases: tuple[type[Any], ...]
) -> None:
    optional_fields = namespace.get("__optional_fields__", [])
    if not optional_fields:
        return
    annotations = namespace.setdefault("__annotations__", {})
    remaining = set(optional_fields)
    for base in bases:
        if not remaining:
            break
        if not hasattr(base, "model_fields"):
            continue
        for fname in list(remaining):
            if fname in base.model_fields and fname not in annotations:
                fi = base.model_fields[fname]
                annotations[fname] = Union[fi.annotation, None]
                namespace[fname] = Field(None, description=fi.description)
                remaining.discard(fname)


def _inject_variable_types(namespace: dict[str, Any], cls_name: str) -> None:
    excluded_class_fields = _VARTYPE_EXCLUDED_CLASS_FIELDS.get(cls_name, set())
    for field_name in namespace.get("__annotations__", {}):
        type_hint = namespace["__annotations__"][field_name]

        if field_name.startswith("_"):
            continue
        if field_name in _VARTYPE_EXCLUDED_FIELDS:
            continue
        if field_name in excluded_class_fields:
            continue
        if type_hint is None or type_hint is typing.Any:
            continue

        origin = get_origin(type_hint)
        args = get_args(type_hint)
        new_type_hint = type_hint

        if origin is list:
            new_type_hint = list[args[0] | VariableType]
        elif origin is dict:
            new_type_hint = dict[args[0] | VariableType, args[1] | VariableType]

        new_type_hint = Union[new_type_hint, VariableType]
        namespace["__annotations__"][field_name] = new_type_hint


class ModelMetaclass(_ModelMetaclass):
    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        _resolve_plural_fields(namespace)
        _expand_optional_fields(namespace, bases)
        _inject_variable_types(namespace, cls_name)
        return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class BaseModel(_BaseModel, metaclass=ModelMetaclass):
    """
    Parent class for all Laktory models offering generic functions and
    properties. This `BaseModel` class is derived from `pydantic.BaseModel`.
    """

    __doc_hide_base__ = True  # hide this class's fields and methods from child docs

    model_config = ConfigDict(
        extra="forbid",
        # `validate_assignment` is required when injecting complex variables to resolve
        # target model and more suitable when models are dynamically updated in code.
        validate_assignment=True,
        # Allow field names (in addition to aliases) to be used in model_validate().
        populate_by_name=True,
    )
    variables: dict[str, Any] = Field(
        default={},
        exclude=True,
        description="Dict of variables to be injected in the model at runtime",
    )
    _inject_vars_cache_key: str | None = PrivateAttr(default=None)
    _inject_vars_cache_value: Any = PrivateAttr(default=None)

    @model_validator(mode="after")
    def variables_self_reference(self) -> Any:
        if self.variables is None:
            return self

        for k, v in self.variables.items():
            if not isinstance(v, str):
                continue

            self_reference = False
            if "${vars." + k + "}" in v or "${var." + k + "}" in v:
                self_reference = True

            if v.startswith("${{"):
                # Regular expression to match 'vars.env' or 'var.env'
                pattern = rf"\bvars?\.{k}\b"

                # Check for match
                if re.search(pattern, v):
                    self_reference = True

            if self_reference:
                raise ValueError(
                    f"Variable `{k}={v}` cannot reference itself or a parent definition of the same name."
                )

        return self

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    @classmethod
    def model_validate_yaml(cls: Type[Model], fp: TextIO, vars=None) -> Model:
        """
        Load model from yaml file object using laktory.yaml.RecursiveLoader. Supports
        reference to external yaml and sql files using `!use`, `!extend` and `!update` tags.
        Path to external files can be defined using model or environment variables.

        Referenced path should always be relative to the file they are referenced from.

        Custom Tags
        -----------
        - `!use {filepath}`:
            Directly inject the content of the file at `filepath`

        - `- !extend {filepath}`:
            Extend the current list with the elements found in the file at `filepath`.
            Similar to python list.extend method.

        - `<<: !update {filepath}`:
            Merge the current dictionary with the content of the dictionary defined at
            `filepath`. Similar to python dict.update method.

        Parameters
        ----------
        fp:
            file object structured as a yaml file
        vars:
            Dict of variables available when parsing filepaths references in yaml files
            i.e. `!use catalog_${vars.env}.yaml`

        Returns
        -------
        :
            Model instance

        Examples
        --------
        ```yaml
        businesses:
          apple:
            symbol: aapl
            address: !use addresses.yaml
            <<: !update common.yaml
            emails:
              - jane.doe@apple.com
              - extend! emails.yaml
          amazon:
            symbol: amzn
            address: !use addresses.yaml
            <<: update! common.yaml
            emails:
              - john.doe@amazon.com
              - extend! emails.yaml
        ```
        """

        data = RecursiveLoader.load(fp, vars=vars)
        return cls.model_validate(data)

    def model_dump_yaml(self, *args, **kwargs):
        return yaml.dump(self.model_dump(*args, **kwargs))

    @classmethod
    def model_validate_json_file(cls: Type[Model], fp: TextIO) -> Model:
        """
        Load model from json file object

        Parameters
        ----------
        fp:
            file object structured as a json file

        Returns
        -------
        :
            Model instance
        """
        data = json.load(fp)
        return cls.model_validate(data)

    # ----------------------------------------------------------------------- #
    # Update                                                                  #
    # ----------------------------------------------------------------------- #

    def update(self, update: dict[Any, Any]) -> None:
        for key, value in update.items():
            if isinstance(self, BaseModel):
                current = getattr(self, key)
            elif isinstance(self, dict):
                current = self.get(key)
            else:
                raise TypeError(f"Unsupported self type: {type(self)}")

            if isinstance(current, BaseModel) and isinstance(value, dict):
                current.update(value)
            elif isinstance(current, dict) and isinstance(value, dict):
                for subkey, subval in value.items():
                    if isinstance(current.get(subkey), BaseModel) and isinstance(
                        subval, dict
                    ):
                        current[subkey].update(subval)
                    else:
                        current[subkey] = subval
            else:
                if isinstance(self, BaseModel):
                    setattr(self, key, value)
                else:
                    self[key] = value

    # ----------------------------------------------------------------------- #
    # Serialization                                                           #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Field Assignment                                                        #
    # ----------------------------------------------------------------------- #

    def _setattr(self, name: str, value: Any) -> None:
        """
        Set a field value bypassing Pydantic's ``validate_assignment`` machinery.

        Use this inside ``@model_validator(mode="after")`` bodies when you need
        to write back to a field without re-triggering the validator chain.
        Calling ``self.field = value`` there would cause infinite recursion
        because ``validate_assignment=True`` re-runs all model validators on
        every attribute write. ``object.__setattr__`` sidesteps that by going
        directly to Python's base attribute-setting primitive, which Pydantic
        does not intercept.

        Do not use this outside of model validators - prefer normal attribute
        assignment so that validation stays active.
        """
        object.__setattr__(self, name, value)
        self.__pydantic_fields_set__.add(name)

    # ----------------------------------------------------------------------- #
    # Variables Injection                                                     #
    # ----------------------------------------------------------------------- #

    def _inject_vars_objs(self) -> dict:
        return {}

    def push_vars(self, update_core_resources=False) -> Any:
        """Push variable values to all child recursively"""

        def _update_model(m):
            if not isinstance(m, BaseModel):
                return
            for k, v in self.variables.items():
                m.variables[k] = m.variables.get(k, v)
            m.push_vars()

        def _push_vars(o):
            if isinstance(o, list):
                for _o in o:
                    _push_vars(_o)
            elif isinstance(o, dict):
                for _o in o.values():
                    _push_vars(_o)
            else:
                _update_model(o)

        for k in type(self).model_fields.keys():
            _push_vars(getattr(self, k))

        if update_core_resources and hasattr(self, "core_resources"):
            for r in self.core_resources:
                if r != self:
                    _push_vars(r)

        return None

    def inject_vars(self, inplace: bool = False, vars: dict = None, objs: dict = None):
        """
        Inject model variables values into a model attributes.

        Parameters
        ----------
        inplace:
            If `True` model is modified in place. Otherwise, a new model
            instance is returned.
        vars:
            A dictionary of variables to be injected in addition to the
            model internal variables.
        objs:
            A dictionary of objects available when resolving expressions.


        Returns
        -------
        :
            Model instance.

        Examples
        --------
        ```py
        from typing import Union

        from laktory import models


        class Cluster(models.BaseModel):
            name: str = None
            size: Union[int, str] = None


        c = Cluster(
            name="cluster-${vars.my_cluster}",
            size="${{ 4 if vars.env == 'prod' else 2 }}",
            variables={
                "env": "dev",
            },
        ).inject_vars()
        print(c)
        # > variables={'env': 'dev'} name='cluster-${vars.my_cluster}' size=2
        ```

        References
        ----------
        * [variables](https://www.laktory.ai/concepts/variables/)
        """

        # Fetching vars
        # Merge first, deep-copy once: _resolve_values mutates mutable values
        # (list/dict) in-place when resolving nested variable references, so both
        # the caller's vars and self.variables values must be protected. Two
        # separate deepcopies (caller then update) would leave self.variables
        # unprotected; merging into a new dict first and copying once covers both.
        vars = deepcopy({**(vars or {}), **self.variables})

        # Fetching objs - subclasses override _inject_vars_objs() to inject
        # context objects (e.g. pipeline, pipeline_node) without circular imports
        _caller_objs = objs
        if objs is None:
            objs = self._inject_vars_objs()
        else:
            objs = {**self._inject_vars_objs(), **objs}

        # Cache check: skip re-resolution when vars and objs haven't changed
        cache_key = None
        if not inplace and _caller_objs is None:
            cache_key = json.dumps(vars, sort_keys=True)
            if self._inject_vars_cache_key == cache_key:
                return self._inject_vars_cache_value.model_copy(deep=True)

        # Create copy
        if not inplace:
            original = self
            self = self.model_copy(deep=True)

        # Inject into field values
        for k in list(self.model_fields_set):
            if k == "variables":
                continue
            o = getattr(self, k)

            if isinstance(o, BaseModel) or isinstance(o, dict) or isinstance(o, list):
                # Mutable objects will be updated in place
                _resolve_values(o, vars, objs)
            else:
                # Simple objects must be updated explicitly
                setattr(self, k, _resolve_value(o, vars, objs))

        # Inject into child resources
        if hasattr(self, "core_resources"):
            for r in self.core_resources:
                if r == self:
                    continue
                r.inject_vars(vars=vars, inplace=True, objs=objs)

        if not inplace:
            if cache_key is not None:
                original._inject_vars_cache_key = cache_key
                original._inject_vars_cache_value = self
            return self

    def inject_vars_into_dump(
        self,
        dump: dict[str, Any],
        inplace: bool = False,
        vars: dict[str, Any] = None,
        objs: dict[str, Any] = None,
    ):
        """
        Inject model variables values into a model dump.

        Parameters
        ----------
        dump:
            Model dump (or any other general purpose mutable object)
        inplace:
            If `True` model is modified in place. Otherwise, a new model
            instance is returned.
        vars:
            A dictionary of variables to be injected in addition to the
            model internal variables.
        objs:
            A dictionary of objects available when resolving expressions.


        Returns
        -------
        :
            Model dump with injected variables.


        Examples
        --------
        ```py
        from laktory import models

        m = models.BaseModel(
            variables={
                "env": "dev",
            },
        )
        data = {
            "name": "cluster-${vars.my_cluster}",
            "size": "${{ 4 if vars.env == 'prod' else 2 }}",
        }
        print(m.inject_vars_into_dump(data))
        # > {'name': 'cluster-${vars.my_cluster}', 'size': 2}
        ```

        References
        ----------
        * [variables](https://www.laktory.ai/concepts/variables/)
        """

        # Setting vars - same merge-then-copy pattern as inject_vars()
        vars = deepcopy({**(vars or {}), **self.variables})

        # Create copy
        if not inplace:
            dump = copy.deepcopy(dump)

        # Inject into field values
        _resolve_values(dump, vars, objs)

        if not inplace:
            return dump
