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
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_serializer
from pydantic import model_validator
from pydantic._internal._model_construction import ModelMetaclass as _ModelMetaclass

from laktory._parsers import _resolve_value
from laktory._parsers import _resolve_values
from laktory._parsers import _snake_to_camel
from laktory.typing import VariableType
from laktory.yaml.recursiveloader import RecursiveLoader

Model = TypeVar("Model", bound="BaseModel")


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

    model_config = ConfigDict(
        extra="forbid",
        # `validate_assignment` is required when injecting complex variables to resolve
        # target model and more suitable when models are dynamically updated in code.
        validate_assignment=True,
    )
    variables: dict[str, Any] = Field(
        default={},
        exclude=True,
        description="Dict of variables to be injected in the model at runtime",
    )
    _camel_serialization: bool = False
    _singular_serialization: bool = False

    @model_serializer(mode="wrap")
    def custom_serializer(self, handler) -> dict[str, Any]:
        dump = handler(self)
        if dump is None:
            return dump

        dump = self._post_serialization(dump)

        camel_serialization = self._camel_serialization
        singular_serialization = self._singular_serialization

        fields = {k: v for k, v in self.model_fields.items()}
        fields = fields | {k: v for k, v in self.model_computed_fields.items()}
        if camel_serialization:
            keys = list(dump.keys())
            for k in keys:
                k_camel = _snake_to_camel(k)
                if k_camel != k:
                    dump[_snake_to_camel(k)] = dump.pop(k)
                    fields[_snake_to_camel(k)] = fields[k]
                    k = _snake_to_camel(k)

                # TODO: Review and optimize (probably brittle)
                #       This is used to rename properties names from snake to
                #       camel keys when using Pulumi backend. Probably also
                #       need to parse list and dicts.
                if isinstance(dump[k], str) and dump[k].startswith("${resources."):
                    values = dump[k].split(".")
                    values[-1] = _snake_to_camel(values[-1])
                    dump[k] = ".".join(values)

        if singular_serialization:
            import inflect

            engine = inflect.engine()
            keys = list(dump.keys())
            for k in keys:
                if k in self.singularizations:
                    # Explicit singularization
                    k_singular = self.singularizations[k] or k
                else:
                    # Automatic singularization
                    k_singular = k
                    if not hasattr(fields[k], "annotation"):
                        continue
                    ann = fields[k].annotation
                    if annotation_contains_list_of_basemodel(ann, BaseModel):
                        k_singular = engine.singular_noun(k) or k

                if k_singular != k:
                    dump[k_singular] = dump.pop(k)

        return dump

    #
    # def _pre_serialization(self):
    #     """"""
    #     pass

    def _post_serialization(self, dump):
        """"""
        return dump

    @model_validator(mode="after")
    def variables_self_reference(self) -> Any:
        if self.variables is None:
            return self

        for k, v in self.variables.items():
            if not isinstance(v, str):
                continue

            self_reference = False
            if "${vars." + k + "}" in v:
                self_reference = True

            if v.startswith("${{"):
                # Regular expression to match 'vars.env'
                pattern = rf"\bvars\.{k}\b"

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
    def model_validate_yaml(cls: Type[Model], fp: TextIO) -> Model:
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

        data = RecursiveLoader.load(fp)
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
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {}

    @property
    def computed_defaults(self) -> dict[str, str]:
        # Dict whose keys are user input fields and whose values are the default/computed values when user input is not
        # provided
        m = {}

        for cfield_name in self.model_computed_fields.keys():
            field_name = cfield_name + "_"
            if field_name in self.model_fields:
                m[cfield_name] = field_name

        return m

    # ----------------------------------------------------------------------- #
    # Serialization                                                           #
    # ----------------------------------------------------------------------- #

    def _configure_serializer(self, camel=False, singular=False):
        self._camel_serialization = camel
        self._singular_serialization = singular
        for k in self.model_fields:
            f = getattr(self, k)
            if isinstance(f, BaseModel):
                f._configure_serializer(camel, singular)
            elif isinstance(f, list):
                for i in f:
                    if isinstance(i, BaseModel):
                        i._configure_serializer(camel, singular)
            elif isinstance(f, dict):
                for i in f.values():
                    if isinstance(i, BaseModel):
                        i._configure_serializer(camel, singular)

    # ----------------------------------------------------------------------- #
    # Validation ByPass                                                       #
    # ----------------------------------------------------------------------- #

    @contextmanager
    def validate_assignment_disabled(self):
        """
        Updating a model attribute inside a model validator when `validate_assignment`
        is `True` causes an infinite recursion by design and must be turned off
        temporarily.
        """
        original_state = self.model_config["validate_assignment"]
        self.model_config["validate_assignment"] = False
        try:
            yield
        finally:
            self.model_config["validate_assignment"] = original_state

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

        for k in self.model_fields.keys():
            _push_vars(getattr(self, k))

        if update_core_resources and hasattr(self, "core_resources"):
            for r in self.core_resources:
                if r != self:
                    _push_vars(r)

        return None

    def inject_vars(self, inplace: bool = False, vars: dict = None):
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
                _resolve_values(o, vars)
            else:
                # Simple objects must be updated explicitly
                setattr(self, k, _resolve_value(o, vars))

        # Inject into child resources
        if hasattr(self, "core_resources"):
            for r in self.core_resources:
                if r == self:
                    continue
                r.inject_vars(vars=vars, inplace=True)

        if not inplace:
            return self

    def inject_vars_into_dump(
        self, dump: dict[str, Any], inplace: bool = False, vars: dict[str, Any] = None
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
        _resolve_values(dump, vars)

        if not inplace:
            return dump
