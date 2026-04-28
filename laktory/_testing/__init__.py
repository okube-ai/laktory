import os
import types
import typing

from .get_dfs import StreamingSource
from .get_dfs import get_df0
from .get_dfs import get_df1
from .paths import Paths


def skip_test(required, extras=None):
    import pytest

    if extras is not None:
        for e in extras:
            required += [e]

    missing = []
    for k in required:
        if not os.getenv(k):
            missing += [k]
    if missing:
        pytest.skip(f"{missing} not available.")


def skip_terraform_plan(extras=None):
    skip_test(
        required=[
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN",
        ],
        extras=extras,
    )


def _get_stack_resource_key(resource) -> str:
    """Derive the StackResources field name for a resource instance by inspecting StackResources annotations.

    Annotation shape: Union[dict[str | VariableType, ResourceClass | VariableType], VariableType]
    We navigate: outer Union → dict (args[0]) → dict value type (dict_args[1]) → inner Union args.
    """
    from laktory.models.stacks.stack import StackResources

    resource_type = type(resource)

    for field_name, field_info in StackResources.model_fields.items():
        annotation = field_info.annotation
        if annotation is None:
            continue
        # Outer Union args: (dict[...], VariableType)
        outer_args = typing.get_args(annotation)
        if not outer_args:
            continue
        # Pick the dict type (skip VariableType entries)
        dict_type = None
        for a in outer_args:
            if typing.get_origin(a) is dict:
                dict_type = a
                break
        if dict_type is None:
            continue
        # dict value type: dict[key_type, value_type] → args[1]
        dict_args = typing.get_args(dict_type)
        if len(dict_args) < 2:
            continue
        value_type = dict_args[1]

        origin = typing.get_origin(value_type)
        is_union = origin is typing.Union or (
            hasattr(types, "UnionType") and isinstance(value_type, types.UnionType)
        )
        candidate_types = typing.get_args(value_type) if is_union else (value_type,)

        if any(
            isinstance(t, type) and issubclass(resource_type, t)
            for t in candidate_types
        ):
            return field_name

    raise ValueError(
        f"No StackResources key found for {resource_type.__name__}. "
        "Ensure the resource type is registered in StackResources."
    )


def plan_resource(resource, env_name="dev"):
    """Wrap a resource in a minimal Stack and run terraform plan.

    Requires DATABRICKS_HOST and DATABRICKS_TOKEN env vars — call
    skip_terraform_plan() before this function in tests.
    """
    from laktory.models.stacks.stack import Stack

    key = _get_stack_resource_key(resource)

    stack = Stack(
        name="test",
        organization="test",
        resources={
            key: {resource.resource_name: resource},
            "providers": {
                "databricks": {
                    "host": os.environ["DATABRICKS_HOST"],
                    "token": os.environ["DATABRICKS_TOKEN"],
                }
            },
        },
        environments={env_name: {}},
    )
    tstack = stack.to_terraform(env_name=env_name)
    tstack.init(flags=["-reconfigure"])
    tstack.plan()
