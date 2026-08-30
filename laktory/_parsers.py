import copy
import json
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


def _resolve_values(o, vars, objs) -> Any:
    """Inject variables into a mutable object"""

    from laktory.models.basemodel import BaseModel

    if isinstance(o, BaseModel):
        o.inject_vars(inplace=True, vars=vars, objs=objs)
    elif isinstance(o, list):
        for i, _o in enumerate(o):
            o[i] = _resolve_values(_o, vars, objs)
    elif isinstance(o, dict):
        for k, _o in o.items():
            o[k] = _resolve_values(_o, vars, objs)
    else:
        o = _resolve_value(o, vars, objs)
    return o


def _resolve_value(o, vars, objs, stringify=False):
    """
    Replace variables in a simple object.

    `stringify`, when `True`, guarantees a `str` is returned: a match that
    resolves to a non-string (dict, list, bool, ...) is JSON-serialized and
    substituted in place of the matched span, instead of the default
    behavior of replacing the entire value with the raw resolved object.
    Used when resolving placeholders inside arbitrary text (e.g. rendered
    file content) rather than a typed model field.
    """

    # Not a string
    if not isinstance(o, str):
        return o

    # Fast exit: both ${vars.X} and ${{ expr }} require "${"; skip regex work
    # for the vast majority of string fields that have no variable syntax.
    if "${" not in o:
        return o

    # Resolve custom patterns
    for pattern, repl in vars.items():
        if not is_pattern(pattern):
            continue
        elif isinstance(o, str) and re.findall(pattern, o, flags=re.IGNORECASE):
            o = re.sub(pattern, repl, o, flags=re.IGNORECASE)

    if not isinstance(o, str):
        return o

    # Resolve ${vars.<name>} or ${var.<name>} syntax
    pattern = re.compile(r"\$\{vars?\.([a-zA-Z_][a-zA-Z0-9_]*)\}")
    for match in pattern.finditer(o):
        # Extract the variable name
        var_name = match.group(1)

        # Resolve the variable value
        resolved_value = _resolve_variable(var_name, vars, objs)

        # Recursively resolve nested variables if variable value is a dict
        # or a list, whether it ends up embedded (stringify) or returned
        # as-is (whole-value replacement)
        if isinstance(resolved_value, (list, dict)):
            resolved_value = _resolve_values(resolved_value, vars, objs)

        # Update the value with the resolved value
        if isinstance(resolved_value, str):
            o = o.replace(match.group(0), resolved_value)
        elif stringify:
            o = o.replace(match.group(0), json.dumps(resolved_value))
        else:
            o = resolved_value

    if not isinstance(o, str):
        return o

    # Resolve ${settings.<name>} or ${setting.<name>} syntax
    pattern = re.compile(r"\$\{settings?\.([a-zA-Z_][a-zA-Z0-9_]*)\}")
    for match in pattern.finditer(o):
        # Extract the setting name
        setting_name = match.group(1)

        # Resolve the setting value
        resolved_value = _resolve_settings(setting_name, vars, objs)

        # Recursively resolve nested variables if variable value is a dict
        # or a list, whether it ends up embedded (stringify) or returned
        # as-is (whole-value replacement)
        if isinstance(resolved_value, (list, dict)):
            resolved_value = _resolve_values(resolved_value, vars, objs)

        # Update the value with the resolved value
        if isinstance(resolved_value, str):
            o = o.replace(match.group(0), resolved_value)
        elif stringify:
            o = o.replace(match.group(0), json.dumps(resolved_value))
        else:
            o = resolved_value

    if not isinstance(o, str):
        return o

    # Resolve ${current_user.<name>} syntax
    pattern = re.compile(r"\$\{current_user\.([a-zA-Z_][a-zA-Z0-9_]*)\}")
    for match in pattern.finditer(o):
        # Extract the attribute name
        attr_name = match.group(1)

        # Resolve the current user value
        resolved_value = _resolve_current_user(attr_name, vars, objs)

        # Recursively resolve nested variables if variable value is a dict
        # or a list, whether it ends up embedded (stringify) or returned
        # as-is (whole-value replacement)
        if isinstance(resolved_value, (list, dict)):
            resolved_value = _resolve_values(resolved_value, vars, objs)

        # Update the value with the resolved value
        if isinstance(resolved_value, str):
            o = o.replace(match.group(0), resolved_value)
        elif stringify:
            o = o.replace(match.group(0), json.dumps(resolved_value))
        else:
            o = resolved_value

    if not isinstance(o, str):
        return o

    # Resolve ${{ <expression> }} syntax
    pattern = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")
    for match in pattern.finditer(o):
        # Extract the variable name
        expr = match.group(1)

        # Resolve the variable value
        resolved_value = _resolve_expression(expr, vars, objs)

        # Update the value with the resolved value
        if isinstance(resolved_value, str):
            o = o.replace(match.group(0), resolved_value)
        elif stringify:
            o = o.replace(match.group(0), json.dumps(resolved_value))
        else:
            o = resolved_value

    return o


def _get_settings_value(name):
    """Look up `name` (case-insensitively) among `laktory._settings.settings` fields."""
    from laktory._settings import settings

    _vals = {k.lower(): getattr(settings, k) for k in settings.model_fields.keys()}
    return _vals.get(name.lower())


def _resolve_variable(name, vars, objs):
    """Resolve a variable name from the variables or environment."""

    # Fetch from model variables
    _vars = {k.lower(): v for k, v in vars.items()}
    value = _vars.get(name.lower())

    # Fetch from env variables
    if value is None:
        _vars = {k.lower(): v for k, v in os.environ.items()}
        value = _vars.get(name.lower())

    # Fetch from laktory settings. This is a documented, backward-compatible
    # alias - `${settings.<name>}` (below) is the explicit, unambiguous
    # syntax and should be preferred, since a model/env variable of the same
    # name silently takes precedence over the settings value here.
    if value is None:
        value = _get_settings_value(name)

    # Value not found returning original value
    if value is None:
        return f"${{vars.{name}}}"  # Default value if not resolved

    # If the resolved value is itself a string with variables, resolve it
    if isinstance(value, str) and ("${" in value or "$${" in value):
        value = _resolve_value(value, vars, objs)

    return value


def _resolve_settings(name, vars, objs):
    """Resolve a `${settings.<name>}` / `${setting.<name>}` reference."""
    value = _get_settings_value(name)

    # Value not found returning original value
    if value is None:
        return f"${{settings.{name}}}"  # Default value if not resolved

    # If the resolved value is itself a string with variables, resolve it
    if isinstance(value, str) and ("${" in value or "$${" in value):
        value = _resolve_value(value, vars, objs)

    return value


def _get_current_user_value(name):
    """Look up `name` on the `laktory._current_user.current_user` singleton.

    Returns `None` if not yet resolved (no `Stack` has found and resolved a
    `${current_user.x}` reference) or if `name` isn't a recognized attribute.
    """
    from laktory._current_user import current_user

    return getattr(current_user, name.lower(), None)


def _resolve_current_user(name, vars, objs):
    """Resolve a `${current_user.<name>}` reference.

    `laktory._current_user.current_user` is populated by `Stack` (see
    `Stack._resolve_user_root` in `laktory/models/stacks/stack.py`) before
    the general variable-resolution pass runs, so by the time this is
    called the value should already be set. Returns the raw template
    unresolved (matching `${settings.x}`'s behavior) if it isn't - e.g. when
    resolving outside a `Stack` context.
    """
    value = _get_current_user_value(name)

    # Value not found returning original value
    if value is None:
        return f"${{current_user.{name}}}"  # Default value if not resolved

    # If the resolved value is itself a string with variables, resolve it
    if isinstance(value, str) and ("${" in value or "$${" in value):
        value = _resolve_value(value, vars, objs)

    return value


def _resolve_expression(expression, vars, objs):
    """Evaluate an inline expression."""
    # Names referenced as `vars.name` / `var.name` inside the expression
    referenced_names = set(
        re.findall(r"\bvars?\.([a-zA-Z_][a-zA-Z0-9_]*)\b", expression)
    )

    # Translate vars.env or var.env to variables_map['env']
    expression = re.sub(
        r"\bvars?\.([a-zA-Z_][a-zA-Z0-9_]*)\b", r"variables_map['\1']", expression
    )

    # Names referenced as `settings.name` / `setting.name`, translated the
    # same way (settings.name -> settings_map['name'])
    referenced_settings = set(
        re.findall(r"\bsettings?\.([a-zA-Z_][a-zA-Z0-9_]*)\b", expression)
    )
    expression = re.sub(
        r"\bsettings?\.([a-zA-Z_][a-zA-Z0-9_]*)\b",
        r"settings_map['\1']",
        expression,
    )

    # Names referenced as `current_user.name`, translated the same way
    # (current_user.name -> current_user_map['name'])
    referenced_current_user = set(
        re.findall(r"\bcurrent_user\.([a-zA-Z_][a-zA-Z0-9_]*)\b", expression)
    )
    expression = re.sub(
        r"\bcurrent_user\.([a-zA-Z_][a-zA-Z0-9_]*)\b",
        r"current_user_map['\1']",
        expression,
    )

    # Prepare a safe evaluation context - shallow copy is sufficient because
    # eval() only reads from variables_map and never mutates var values.
    # Only the variables actually referenced by the expression are
    # recursively re-resolved (mirroring what _resolve_variable() already
    # does for plain `${vars.x}` substitutions), so eval() never sees a raw,
    # still-unresolved `${vars.x}` indirection. Resolving the full vars dict
    # eagerly instead would re-enter resolution of the very entry currently
    # being evaluated and recurse indefinitely.
    local_context = dict(vars)
    for name in referenced_names:
        if name in local_context:
            local_context[name] = _resolve_variable(name, vars, objs)

    settings_context = {name: _get_settings_value(name) for name in referenced_settings}
    current_user_context = {
        name: _get_current_user_value(name) for name in referenced_current_user
    }

    locals = {
        "variables_map": local_context,
        "settings_map": settings_context,
        "current_user_map": current_user_context,
    }

    if objs is not None:
        for k, v in objs.items():
            locals[k] = v

    try:
        # Allow Python evaluation of conditionals and operations
        return eval(expression, {}, locals)
    except Exception as e:
        raise ValueError(f"Error evaluating expression '{expression}': {e}")
