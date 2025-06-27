import copy
import os
import re
from typing import Any

# --------------------------------------------------------------------------- #
# String Parsing                                                              #
# --------------------------------------------------------------------------- #


def _snake_to_camel(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


# def remove_empty(d):
#     if isinstance(d, dict):
#         keys = list(d.keys())
#         values = list(d.values())
#         for key, value in zip(keys, values):
#             if value in [None, [], {}]:
#                 del d[key]
#             else:
#                 d[key] = remove_empty(d[key])
#
#     elif isinstance(d, list):
#         for i, item in enumerate(d):
#             if item in [None, [], {}]:
#                 del d[i]
#             else:
#                 d[i] = remove_empty(item)
#
#     return d


def camelize_keys(d, parent=None, excluded_parents=None):
    if isinstance(d, dict):
        if parent and excluded_parents and parent in excluded_parents:
            return d
        keys = list(d.keys())
        values = list(d.values())
        for key, value in zip(keys, values):
            new_key = _snake_to_camel(key)
            d[new_key] = camelize_keys(
                value, parent=key, excluded_parents=excluded_parents
            )
            if new_key != key:
                del d[key]

    elif isinstance(d, list):
        for i, item in enumerate(d):
            d[i] = camelize_keys(item)
    else:
        pass
    return d


# --------------------------------------------------------------------------- #
# Dict Parsing                                                                #
# --------------------------------------------------------------------------- #


def merge_dicts(d1: dict, d2: dict) -> dict:
    dm = copy.deepcopy(d1)

    def _merge_dicts(d1, d2):
        for key, value in d2.items():
            if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
                _merge_dicts(d1[key], value)
            elif key in d1 and isinstance(d1[key], list) and isinstance(value, dict):
                for index, sub_value in value.items():
                    idx = int(index)
                    if isinstance(d1[key][idx], dict):
                        _merge_dicts(d1[key][idx], sub_value)
                    else:
                        d1[key][idx] = sub_value
            else:
                d1[key] = value

    _merge_dicts(dm, d2)

    return dm


# --------------------------------------------------------------------------- #
# Variable Resolution                                                         #
# --------------------------------------------------------------------------- #


def is_pattern(s):
    return r"\$\{" in s


def _resolve_values(o, vars) -> Any:
    """Inject variables into a mutable object"""

    from laktory.models.basemodel import BaseModel

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

    # Fetch from model variables
    _vars = {k.lower(): v for k, v in vars.items()}
    value = _vars.get(name.lower())

    # Fetch from env variables
    if value is None:
        _vars = {k.lower(): v for k, v in os.environ.items()}
        value = _vars.get(name.lower())

    # Fetch from laktory settings
    if value is None:
        from laktory._settings import settings

        _vars = {k.lower(): getattr(settings, k) for k in settings.model_fields.keys()}
        value = _vars.get(name.lower())

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
    local_context = copy.deepcopy(vars)

    try:
        # Allow Python evaluation of conditionals and operations
        return eval(expression, {}, {"variables_map": local_context})
    except Exception as e:
        raise ValueError(f"Error evaluating expression '{expression}': {e}")
