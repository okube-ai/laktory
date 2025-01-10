import copy
import json
import os
import re
from contextlib import contextmanager
from copy import deepcopy
from typing import Any
from typing import TextIO
from typing import TypeVar

import inflect
import yaml
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_serializer

from laktory._parsers import _snake_to_camel

Model = TypeVar("Model", bound="BaseModel")


def is_pattern(s):
    return r"\$\{" in s


def _resolve_values(o, vars) -> Any:
    """Inject variables into a mutable object"""
    if isinstance(o, BaseModel):
        o.inject_vars(inplace=True, vars=vars)
    elif isinstance(o, list):
        for i, _o in enumerate(o):
            o[i] = _resolve_values(_o, vars)
    elif isinstance(o, dict):
        for k, _o in o.items():
            o[k] = _resolve_values(_o, vars)
    else:
        o = _resolve_value(o, vars)
    return o


def _resolve_value(o, vars):
    """Replace variables in a simple object"""

    # Not a string
    if not isinstance(o, str):
        return o

    # Resolve custom patterns
    for pattern, repl in vars.items():
        if not is_pattern(pattern):
            continue
        # if o == pattern:
        #     o = repl  # required where d is not a string (bool or resource object)
        elif isinstance(o, str) and re.findall(pattern, o, flags=re.IGNORECASE):
            o = re.sub(pattern, repl, o, flags=re.IGNORECASE)

    if not isinstance(o, str):
        return o

    # Resolve ${vars.<name>} syntax
    pattern = re.compile(r"\$\{vars\.([a-zA-Z_][a-zA-Z0-9_]*)\}")
    for match in pattern.finditer(o):
        # Extract the variable name
        var_name = match.group(1)

        # Resolve the variable value
        resolved_value = _resolve_variable(var_name, vars)

        # Update the value with the resolved value
        if isinstance(resolved_value, str):
            o = o.replace(match.group(0), resolved_value)
        else:
            o = resolved_value

            # Recursively resolve element if variable value is a dict or a list
            if isinstance(o, (list, dict)):
                o = _resolve_values(o, vars)

    if not isinstance(o, str):
        return o

    # Resolve ${{ <expression> }} syntax
    pattern = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")
    for match in pattern.finditer(o):
        # Extract the variable name
        expr = match.group(1)

        # Resolve the variable value
        resolved_value = _resolve_expression(expr, vars)

        # Update the value with the resolved value
        if isinstance(resolved_value, str):
            o = o.replace(match.group(0), resolved_value)
        else:
            o = resolved_value

    return o


def _resolve_variable(name, vars):
    """Resolve a variable name from the variables or environment."""

    value = vars.get(name, os.getenv(name))

    # Value not found returning original value
    if value is None:
        return f"${{vars.{name}}}"  # Default value if not resolved

    # If the resolved value is itself a string with variables, resolve it
    if isinstance(value, str) and ("${" in value or "$${" in value):
        value = _resolve_value(value, vars)

    return value


def _resolve_expression(expression, vars):
    """Evaluate an inline expression."""
    # Translate vars.env to variables_map['env']
    expression = re.sub(
        r"\bvars\.([a-zA-Z_][a-zA-Z0-9_]*)\b", r"variables_map['\1']", expression
    )

    # Prepare a safe evaluation context
    local_context = deepcopy(vars)

    try:
        # Allow Python evaluation of conditionals and operations
        return eval(expression, {}, {"variables_map": local_context})
    except Exception as e:
        raise ValueError(f"Error evaluating expression '{expression}': {e}")


class BaseModel(_BaseModel):
    """
    Parent class for all Laktory models offering generic functions and
    properties. This `BaseModel` class is derived from `pydantic.BaseModel`.

    Attributes
    ----------
    variables:
        Variable values to be resolved when using `inject_vars` method.
    """

    # model_config = ConfigDict(validate_assignment=True)
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,  # Required when injecting complex variables to resolve target model
    )
    variables: dict[str, Any] = Field(default={}, exclude=True)
    _camel_serialization: bool = False
    _singular_serialization: bool = False

    @model_serializer(mode="wrap")
    def custom_serializer(self, handler) -> dict[str, Any]:
        dump = handler(self)
        if dump is None:
            return dump

        camel_serialization = self._camel_serialization
        singular_serialization = self._singular_serialization

        fields = {k: v for k, v in self.model_fields.items()}
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
            engine = inflect.engine()
            keys = list(dump.keys())
            for k in keys:
                if k in self.singularizations:
                    # Explicit singularization
                    k_singular = self.singularizations[k] or k
                else:
                    # Automatic singularization
                    k_singular = k
                    ann = str(fields[k].annotation)
                    if ann.startswith("list[laktory.models"):
                        k_singular = engine.singular_noun(k) or k

                if k_singular != k:
                    dump[k_singular] = dump.pop(k)

        return dump

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    @classmethod
    def model_validate_yaml(cls, fp: TextIO) -> Model:
        """
            Load model from yaml file object. Other yaml files can be referenced
            using the ${include.other_yaml_filepath} syntax. You can also merge
            lists with -< ${include.other_yaml_filepath} and dictionaries with
            <<: ${include.other_yaml_filepath}. Including multi-lines and
            commented SQL files is also possible.

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
            address: ${include.addresses.yaml}
            <<: ${include.common.yaml}
            emails:
              - jane.doe@apple.com
              -< ${include.emails.yaml}
          amazon:
            symbol: amzn
            address: ${include.addresses.yaml}
            <<: ${include.common.yaml}
            emails:
              - john.doe@amazon.com
              -< ${include.emails.yaml}
        ```
        """

        if hasattr(fp, "name"):
            dirpath = os.path.dirname(fp.name)
        else:
            dirpath = "./"

        def inject_includes(lines):
            _lines = []
            for line in lines:
                line = line.replace("\n", "")
                indent = " " * (len(line) - len(line.lstrip()))
                if line.strip().startswith("#"):
                    continue

                if "${include." in line:
                    pattern = r"\{include\.(.*?)\}"
                    matches = re.findall(pattern, line)
                    path = matches[0]
                    path0 = path
                    if not os.path.isabs(path):
                        path = os.path.join(dirpath, path)
                    path_ext = path.split(".")[-1]
                    if path_ext not in ["yaml", "yml", "sql"]:
                        raise ValueError(
                            f"Include file of format {path_ext} ({path}) is not supported."
                        )

                    # Merge include
                    if "<<: ${include." in line or "-< ${include." in line:
                        with open(path, "r", encoding="utf-8") as _fp:
                            new_lines = _fp.readlines()
                            _lines += [
                                indent + __line for __line in inject_includes(new_lines)
                            ]

                    # Direct Include
                    else:
                        if path.endswith(".sql"):
                            with open(path, "r", encoding="utf-8") as _fp:
                                new_lines = _fp.read()
                            _lines += [
                                line.replace(
                                    "${include." + path0 + "}",
                                    '"' + new_lines.replace("\n", "\\n") + '"',
                                )
                            ]

                        elif path.endswith(".yaml") or path.endswith("yml"):
                            indent = indent + " " * 2
                            _lines += [line.split("${include")[0]]
                            with open(path, "r", encoding="utf-8") as _fp:
                                new_lines = _fp.readlines()
                            _lines += [
                                indent + __line for __line in inject_includes(new_lines)
                            ]

                else:
                    _lines += [line]

            return _lines

        lines = inject_includes(fp.readlines())
        data = yaml.safe_load("\n".join(lines))

        return cls.model_validate(data)

    def model_dump_yaml(self, *args, **kwargs):
        return yaml.dump(self.model_dump(*args, **kwargs))

    @classmethod
    def model_validate_json_file(cls, fp: TextIO) -> Model:
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
        is `True` caused an infinite recursion and must be turned off temporarily.
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
        Inject variables values into a model attributes.

        There are 2 types of variables:

        - User defined variables expressed as `${vars.variable_name}` and
          defined in `self.variables` (pulled from stack variables) or as
          environment variables. Stack variables have priority over environment
          variables.
        - Resources output properties expressed as
         `${resources.resource_name.output}`.

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
        """

        # Fetching vars
        if vars is None:
            vars = {}
        vars = deepcopy(vars)
        vars.update(self.variables)
        vars = deepcopy(vars)

        # Create copy
        if not inplace:
            self = self.model_copy(deep=True)

        # Inject into field values
        for k in self.model_fields_set:
            if k == "variables":
                continue
            o = getattr(self, k)
            if isinstance(o, BaseModel) or isinstance(o, dict) or isinstance(o, list):
                _resolve_values(o, vars)
            else:
                setattr(self, k, _resolve_value(o, vars))

        if not inplace:
            return self

    def inject_vars_into_dump(
        self, dump: dict[str, Any], inplace: bool = False, vars: dict[str, Any] = None
    ):
        """
        Inject variables values into a model dump.

        There are 2 types of variables:

        - User defined variables expressed as `${vars.variable_name}` and
          defined in `self.variables` (pulled from stack variables) or as
          environment variables. Stack variables have priority over environment
          variables.
        - Resources output properties expressed as
         `${resources.resource_name.output}`.

        Parameters
        ----------
        dump:
            Model dump (or any other general purpose dictionary)
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
