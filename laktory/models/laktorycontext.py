import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    pass


@dataclass
class LaktoryContext:
    """
    Runtime context object optionally injected into user-supplied functions by Laktory.

    Passed as the `laktory_context` keyword argument when the called function's
    signature explicitly declares it. Contains references to the
    pipeline objects active at call time.

    To opt in, add `laktory_context=None` to your function signature:

    ```python
    from laktory.models import LaktoryContext


    def my_write(df, laktory_context: LaktoryContext = None) -> None:
        sink = laktory_context.sink
        node = laktory_context.node
        ...
    ```

    Attributes
    ----------
    node:
        Parent PipelineNode, or None when called outside a pipeline.
    pipeline:
        Parent Pipeline, or None when called outside a pipeline.
    sink:
        Current data sink. Populated by DataSinkWriter; None in DataFrameMethod.
    """

    node: Any = None  # PipelineNode
    pipeline: Any = None  # Pipeline
    sink: Any = None  # DataSink


def _build_laktory_context_kwargs(func, context: LaktoryContext) -> dict:
    """
    Return ``{'laktory_context': context}`` if *func* has an explicit
    ``laktory_context`` parameter, otherwise return ``{}``.

    Deliberately does not inject on ``**kwargs``-only signatures: many
    DataFrame methods (e.g. ``df.select``) accept ``**kwargs`` as named
    column expressions and would misinterpret a ``LaktoryContext`` value.
    """
    try:
        params = inspect.signature(func).parameters
    except (ValueError, TypeError):
        return {}
    return {"laktory_context": context} if "laktory_context" in params else {}
