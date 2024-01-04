import pulumi
import yaml
import json
import os
from typing import Any
from typing import TypeVar
from typing import TextIO
from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict
from pydantic import Field

Model = TypeVar("Model", bound="BaseModel")


def _snake_to_camel(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def camelize_keys(d):
    if isinstance(d, dict):
        keys = list(d.keys())
        values = list(d.values())
        for key, value in zip(keys, values):
            new_key = _snake_to_camel(key)
            d[new_key] = camelize_keys(value)
            if new_key != key:
                del d[key]

    elif isinstance(d, list):
        for i, item in enumerate(d):
            d[i] = camelize_keys(item)
    else:
        pass
    return d


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

    @classmethod
    def model_validate_yaml(cls, fp: TextIO) -> Model:
        """
        Load model from yaml file object

        Parameters
        ----------
        fp:
            file object structured as a yaml file

        Returns
        -------
        :
            Model instance
        """
        data = yaml.safe_load(fp)
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

    def model_dump(self, *args, keys_to_camel_case=False, **kwargs):
        """TODO"""
        d = super().model_dump(*args, **kwargs)

        if keys_to_camel_case:
            d = camelize_keys(d)

        return d

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def inject_vars(self, d: dict, target=None) -> dict[str, Any]:
        """
        Inject variables values into a dictionary (generally model dump).

        There are 3 types of variables:
            - User defined variables expressed as `${var.variable_name}` and
              defined in `self.variables`.
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

        # Build available variables
        _vars = {}
        _pvars = {}

        if target == "pulumi_yaml":
            _vars["${resources."] = "${"
        elif target == "terraform":
            # TODO: Review
            raise NotImplementedError()
            # _vars["${catalogs."] = "${databricks_catalog."
            # _vars["${clusters."] = "${databricks_cluster."
            # _vars["${groups."] = "${databricks_group."
            # _vars["${jobs."] = "${databricks_job."
            # _vars["${notebooks."] = "${databricks_notebook."
            # _vars["${pipelines."] = "${databricks_pipeline."
            # _vars["${schemas."] = "${databricks_schema."
            # _vars["${service_principals."] = "${databricks_service_principal."
            # _vars["${secret_scopes."] = "${databricks_secret_scope."
            # _vars["${sql_queries."] = "${databricks_sql_query."
            # _vars["${tables."] = "${databricks_sql_table."
            # _vars["${users."] = "${databricks_user."
            # _vars["${warehouses."] = "${databricks_sql_endpoint."
            # _vars["${workspace_files."] = "${databricks_workspace_file."
        elif target == "pulumi_py":
            # _vars["${resources."] = "${var."
            pass

        # User-defined variables
        for k, v in self.variables.items():
            if isinstance(v, pulumi.Output):
                _vars[f"${{var.{k}}}"] = f"{{_pargs_{k}}}"
                _pvars[f"_pargs_{k}"] = v
            else:
                _vars[f"${{var.{k}}}"] = v

        # Environment variables
        for k, v in os.environ.items():
            _vars[f"${{var.{k}}}"] = v

        # Pulumi resource outputs
        for k, v in pulumi_outputs.items():
            _vars[f"${{resources.{k}}}"] = v

        # Pulumi resources
        for k, v in pulumi_resources.items():
            _vars[f"${{resources.{k}}}"] = v

        def search_and_replace(d, old_value, new_val):
            if isinstance(d, dict):
                for key, value in d.items():
                    d[key] = search_and_replace(value, old_value, new_val)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = search_and_replace(item, old_value, new_val)
            elif d == old_value:  # required where d is not a string (bool)
                d = new_val
            elif isinstance(d, str) and old_value in d:
                d = d.replace(old_value, new_val)
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
        for var_key, var_value in _vars.items():
            d = search_and_replace(d, var_key, var_value)

        # Build pulumi output function where required
        d = apply_pulumi(d)

        return d
