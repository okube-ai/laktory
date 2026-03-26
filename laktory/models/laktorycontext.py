import inspect
from typing import TYPE_CHECKING
from typing import Any

from pydantic import Field
from pydantic import SkipValidation

from laktory.models.basemodel import BaseModel

if TYPE_CHECKING:
    pass


class LaktoryContext(BaseModel):
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

        return df
    ```
    """

    node: SkipValidation[Any] = Field(
        None, description="Parent PipelineNode, or None when called outside a pipeline."
    )  # PipelineNode
    pipeline: SkipValidation[Any] = Field(
        None, description="Parent Pipeline, or None when called outside a pipeline."
    )  # Pipeline
    sink: SkipValidation[Any] = Field(
        None, description="Current data sink."
    )  # BaseDataSink  (set by CustomWriter)
    source: SkipValidation[Any] = Field(
        None, description="Current data source."
    )  # BaseDataSource (set by CustomReader)


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
