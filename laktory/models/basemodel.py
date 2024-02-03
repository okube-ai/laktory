import pulumi
import yaml
import json
import os
import re
import inflect
from typing import Any
from typing import TypeVar
from typing import TextIO
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_serializer
from laktory._settings import settings
from laktory._parsers import _snake_to_camel

Model = TypeVar("Model", bound="BaseModel")


def is_pattern(s):
    return r"\$\{" in s


class BaseModel(_BaseModel):
    """
    Parent class for all Laktory models offering generic functions and
    properties. This `BaseModel` class is derived from `pydantic.BaseModel`.

    Attributes
    ----------
    variables:
        Variable values to be resolved when using `inject_vars` method.
    """

    model_config = ConfigDict(extra="forbid")
    variables: dict[str, Any] = Field(default={}, exclude=True)

    @model_serializer(mode="wrap")
    def camel_serializer(self, handler) -> dict[str, Any]:
        dump = handler(self)
        if dump is None:
            return dump

        if settings.camel_serialization:
            keys = list(dump.keys())
            for k in keys:
                k_camel = _snake_to_camel(k)
                if k_camel != k:
                    dump[_snake_to_camel(k)] = dump.pop(k)

        if settings.singular_serialization:
            engine = inflect.engine()
            fields = self.model_fields
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

    @classmethod
    def model_validate_yaml(cls, fp: TextIO) -> Model:
        """
        Load model from yaml file object. Other yaml files can be referenced
        using the ${include.other_yaml_filepath} syntax.

        Parameters
        ----------
        fp:
            file object structured as a yaml file

        Returns
        -------
        :
            Model instance
        """

        if hasattr(fp, "name"):
            dirpath = os.path.dirname(fp.name)
        else:
            dirpath = "./"

        def inject_includes(d):
            if isinstance(d, dict):
                for key, value in d.items():
                    d[key] = inject_includes(value)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = inject_includes(item)
            elif "${include." in str(d):
                path = d.replace("${include.", "")[:-1]
                if not os.path.isabs(path):
                    path = os.path.join(dirpath, path)
                with open(path, "r") as _fp:
                    d = yaml.safe_load(_fp)
                    d = inject_includes(d)
            return d

        data = inject_includes(yaml.safe_load(fp))

        return cls.model_validate(data)

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
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def inject_vars(self, d: dict) -> dict[str, Any]:
        """
        Inject variables values into a dictionary (generally model dump).

        There are 3 types of variables:

        - User defined variables expressed as `${vars.variable_name}` and
          defined in `self.variables` or in environment variables.
        - Pulumi resources expressed as `${resources.resource_name}`. These
          are available from `laktory.pulumi_resources` and are populated
          automatically by Laktory.
        - Pulumi resources output properties expressed as
         `${resources.resource_name.output}`. These are available from
         `laktory.pulumi_outputs` and are populated automatically by
          Laktory.

        Pulumi Outputs are also supported as variable values.

        Parameters
        ----------
        d:
            Model dump

        Returns
        -------
        :
            Dump in which variable expressions have been replaced with their
            values.
        """

        from laktory.models.resources.pulumiresource import pulumi_outputs
        from laktory.models.resources.pulumiresource import pulumi_resources

        # Build patterns
        _patterns = {}
        _vars = {}
        _pvars = {}

        # User-defined variables
        for k, v in self.variables.items():
            _k = k
            if not is_pattern(_k):
                _k = f"${{vars.{_k}}}"
            if isinstance(v, pulumi.Output):
                _vars[_k] = f"{{_pargs_{k}}}"
                _pvars[f"_pargs_{k}"] = v
            else:
                _vars[_k] = v

        # Environment variables
        for k, v in os.environ.items():
            _vars[f"${{vars.{k}}}"] = v

        # Pulumi resource outputs
        for k, v in pulumi_outputs.items():
            _vars[f"${{resources.{k}}}"] = v

        # Pulumi resources
        for k, v in pulumi_resources.items():
            _vars[f"${{resources.{k}}}"] = v

        # Create patterns
        keys = list(_vars.keys())
        for k in keys:
            v = _vars[k]
            if isinstance(v, str) and not is_pattern(k):
                pattern = re.escape(k)
                pattern = rf"{pattern}"
                _vars[pattern] = _vars.pop(k)

        def search_and_replace(d, pattern, repl):
            if isinstance(d, dict):
                for key, value in d.items():
                    # if isinstance(key, str) and re.findall(pattern, key, flags=re.IGNORECASE):
                    #     k2 = re.sub(pattern, repl, key, flags=re.IGNORECASE)
                    #     d[k2] = search_and_replace(value, pattern, repl)
                    #     if key != k2:
                    #         del d[key]
                    # else:
                    d[key] = search_and_replace(value, pattern, repl)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = search_and_replace(item, pattern, repl)
            elif (
                d == pattern
            ):  # required where d is not a string (bool or resource object)
                d = repl
            elif isinstance(d, str) and re.findall(pattern, d, flags=re.IGNORECASE):
                d = re.sub(pattern, repl, d, flags=re.IGNORECASE)

            return d

        def apply_pulumi(d):
            if isinstance(d, dict):
                for key, value in d.items():
                    d[key] = apply_pulumi(value)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = apply_pulumi(item)
            elif isinstance(d, str) and "_pargs_" in d:
                d = pulumi.Output.all(**_pvars).apply(
                    lambda args, _d=d: _d.format_map(args)
                )
            else:
                pass
            return d

        # Replace variable with their values (except for pulumi output)
        for pattern, repl in _vars.items():
            d = search_and_replace(d, pattern, repl)

        # Build pulumi output function where required
        d = apply_pulumi(d)

        return d
