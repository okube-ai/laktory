#

import copy
import json
import re
import typing
from contextlib import contextmanager
from copy import deepcopy
from typing import Any
from typing import TextIO
from typing import Type
from typing import TypeVar
from typing import Union
from typing import get_args
from typing import get_origin

import yaml  # TODO: Move into functions?
from pydantic import AliasChoices
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from pydantic._internal._model_construction import ModelMetaclass as _ModelMetaclass

from laktory._parsers import _resolve_value
from laktory._parsers import _resolve_values
from laktory.typing import VariableType
from laktory.yaml.recursiveloader import RecursiveLoader

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


class ModelMetaclass(_ModelMetaclass):
    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        # Resolve PluralField specs: inject plural validation alias using the field name
        for fname in list(namespace.keys()):
            value = namespace[fname]
            if isinstance(value, _PluralFieldSpec):
                plural = value.plural or (
                    fname if fname.endswith("s") else (fname + "s")
                )
                fi = value.field_info
                alias = AliasChoices(plural)
                fi.validation_alias = alias
                fi._attributes_set["validation_alias"] = alias
                namespace[fname] = fi

        # Add var as a possible type hint of each model field to support variables injection
        for field_name in namespace.get("__annotations__", {}):
            type_hint = namespace["__annotations__"][field_name]

            if field_name.startswith("_"):
                continue

            if field_name in [
                "variables",
                "dataframe_backend",  # TODO: Review why this is required
            ]:
                continue

            if cls_name in ["UnityCatalogDataSink", "HiveMetastoreDataSink"]:
                if field_name in [
                    "type",  # this is required to properly select the type of sink
                ]:
                    continue

            if type_hint is None:
                continue

            if type_hint is typing.Any:
                continue

            origin = get_origin(type_hint)
            args = get_args(type_hint)
            new_type_hint = type_hint

            if origin is list:
                new_type_hint = list[tuple([args[0] | VariableType])]

            elif origin is dict:
                new_type_hint = dict[args[0] | VariableType, args[1] | VariableType]

            new_type_hint = Union[new_type_hint, VariableType]

            namespace["__annotations__"][field_name] = new_type_hint

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
    # Validation ByPass                                                       #
    # ----------------------------------------------------------------------- #

    @contextmanager
    def validate_assignment_disabled(self):
        """
        Updating a model attribute inside a model validator when `validate_assignment`
        is `True` causes an infinite recursion by design and must be turned off
        temporarily.

        Pydantic v2 memoizes setattr handlers at the class level. If a field's handler
        was memoized as the validate_assignment handler (because a previous setattr ran
        while validate_assignment=True), it would still call validate_assignment() even
        after setting model_config["validate_assignment"] = False. We clear and restore
        the memoized handlers to force re-evaluation with the disabled config.
        """
        cls = self.__class__
        original_state = cls.model_config["validate_assignment"]
        handlers = getattr(cls, "__pydantic_setattr_handlers__", None)
        original_handlers = dict(handlers) if handlers is not None else None
        cls.model_config["validate_assignment"] = False
        if handlers is not None:
            handlers.clear()
        try:
            yield
        finally:
            cls.model_config["validate_assignment"] = original_state
            if handlers is not None and original_handlers is not None:
                handlers.update(original_handlers)

    # ----------------------------------------------------------------------- #
    # Variables Injection                                                     #
    # ----------------------------------------------------------------------- #

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
        if vars is None:
            vars = {}

        vars = deepcopy(vars)
        vars.update(self.variables)

        # Fetching objs
        if objs is None:
            objs = {}

        # TODO: Review implementation as it results in serious performance hits
        from laktory.models.pipeline import Pipeline
        from laktory.models.pipeline import PipelineNode

        if isinstance(self, Pipeline):
            objs["pipeline"] = self

        if isinstance(self, PipelineNode):
            objs["pipeline_node"] = self

        # Create copy
        if not inplace:
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

        # Setting vars
        if vars is None:
            vars = {}
        vars = deepcopy(vars)
        vars.update(self.variables)

        # Create copy
        if not inplace:
            dump = copy.deepcopy(dump)

        # Inject into field values
        _resolve_values(dump, vars, objs)

        if not inplace:
            return dump
