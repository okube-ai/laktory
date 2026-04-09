import importlib
from typing import Any

import narwhals as nw
from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.laktorycontext import LaktoryContext
from laktory.models.laktorycontext import _build_laktory_context_kwargs
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


class CustomWriter(BaseModel, PipelineChild):
    """
    Definition of a custom write function to be called when writing a DataFrame
    to a sink. Gives the user full control over how data is written.

    Laktory manages the streaming query lifecycle (foreachBatch wrapping, trigger,
    checkpoint, start/await) while the user-supplied function handles the actual
    write logic.

    The function is called as:

    ```python
    # func(df, *func_args, **func_kwargs)
    ```

    where `df` is the DataFrame (native or Narwhals depending on the sink's
    `dataframe_api` setting). Laktory optionally injects a `laktory_context`
    keyword argument containing pipeline runtime objects — declare it in your
    function signature to opt in.

    Examples
    --------
    ```py
    from laktory import models

    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        custom_writer={
            "func_name": "mypackage.etl.my_write",
            "func_kwargs": {"extra_tag": "production"},
        },
    )
    # sink.write(df)
    ```

    ```py
    # mypackage/etl.py
    from laktory.models import LaktoryContext


    def my_write(
        df, extra_tag="default", laktory_context: LaktoryContext = None
    ) -> None:
        sink = laktory_context.sink  # access sink coordinates without re-hardcoding
        df.write.format("delta").mode("append").save(sink.path)
    ```
    """

    func_args: list[Any] = Field(
        [],
        description="Positional arguments passed to the function after the DataFrame.",
    )
    func_kwargs: dict[str, Any] = Field(
        {},
        description="Keyword arguments passed to the function.",
    )
    func_name: str = Field(
        ...,
        description=(
            "Fully qualified importable module path to the callable "
            "(e.g. 'mypackage.etl.my_write'). "
            "The function is imported at runtime via `importlib` and called with the "
            "DataFrame as its first argument. "
        ),
    )

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _resolve_func(self):
        """Dynamically import and return the callable from its dotted path."""
        module_path, func_name = self.func_name.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)

    def execute(self, df, context: LaktoryContext = None) -> None:
        """
        Invoke the user-supplied function with the DataFrame, configured
        args/kwargs, and optionally a `laktory_context` object.

        Parameters
        ----------
        df:
            Input DataFrame (native or Narwhals, as prepared by the caller).
        context:
            Pre-built LaktoryContext to inject. When ``None`` (the default),
            the context is built from the ``_parent`` chain at call time.
            Callers that run inside a ``foreachBatch`` lambda should pre-build
            the context before the lambda and pass it here so that the
            ``_parent`` references are captured while they are still intact.
        """
        func = self._resolve_func()

        if not isinstance(df, (nw.DataFrame, nw.LazyFrame)):
            df = nw.from_native(df)

        if self.dataframe_api == "NATIVE":
            df = nw.to_native(df)

        func_log = f"{self.func_name}("
        func_log += ",".join([str(a) for a in self.func_args])
        func_log += ",".join([f"{k}={v}" for k, v in self.func_kwargs.items()])
        func_log += f") with df type {type(df)}"
        logger.info(f"Writing df with {func_log}")

        if context is None:
            context = LaktoryContext(
                node=self.parent_pipeline_node,
                pipeline=self.parent_pipeline,
                sink=self.parent,
            )
        call_kwargs = {
            **self.func_kwargs,
            **_build_laktory_context_kwargs(func, context),
        }
        func(df, *self.func_args, **call_kwargs)
