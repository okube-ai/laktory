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
    signature declares it (or accepts ``**kwargs``). Contains references to the
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
    Return ``{'laktory_context': context}`` if *func* accepts a
    ``laktory_context`` parameter (or ``**kwargs``), otherwise return ``{}``.
    """
    try:
        params = inspect.signature(func).parameters
    except (ValueError, TypeError):
        return {}
    accepts = "laktory_context" in params or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    return {"laktory_context": context} if accepts else {}
