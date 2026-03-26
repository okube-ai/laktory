import importlib
from typing import Any

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.laktorycontext import LaktoryContext
from laktory.models.laktorycontext import _build_laktory_context_kwargs
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


class CustomReader(BaseModel, PipelineChild):
    """
    Definition of a custom read function used by `CustomDataSource`. Gives the
    user full control over how data is read.

    The function must return a DataFrame (native or Narwhals). Laktory optionally injects
    a `laktory_context` keyword argument — declare it in your function signature
    to opt in:

    ```python
    def my_read(laktory_context=None):
        source = laktory_context.source
        node = laktory_context.node

        return df
    ```

    Examples
    --------
    ```py
    from laktory import models

    source = models.CustomDataSource(
        custom_reader={
            "func_name": "mypackage.etl.my_read",
            "func_kwargs": {"table": "catalog.schema.my_table"},
        },
    )
    # df = source.read()
    ```

    ```py
    # mypackage/etl.py
    from laktory.models import LaktoryContext


    def my_read(table=None, laktory_context: LaktoryContext = None):
        import polars as pl

        return pl.scan_delta(table)
    ```
    """

    func_args: list[Any] = Field(
        [],
        description="Positional arguments passed to the function.",
    )
    func_kwargs: dict[str, Any] = Field(
        {},
        description="Keyword arguments passed to the function.",
    )
    func_name: str = Field(
        ...,
        description=(
            "Fully qualified importable module path to the callable "
            "(e.g. 'mypackage.etl.my_read'). "
            "The function is imported at runtime via `importlib` and must return "
            "a DataFrame (native or Narwhals)."
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

    def execute(self):
        """
        Invoke the user-supplied function with configured args/kwargs and
        optionally a `laktory_context` object.

        Returns
        -------
        :
            DataFrame returned by the user function (native or Narwhals).
        """
        func = self._resolve_func()

        func_log = f"{self.func_name}("
        func_log += ",".join([str(a) for a in self.func_args])
        func_log += ",".join([f"{k}={v}" for k, v in self.func_kwargs.items()])
        func_log += ")"
        logger.info(f"Reading with {func_log}")

        context = LaktoryContext(
            node=self.parent_pipeline_node,
            pipeline=self.parent_pipeline,
            source=self.parent,
        )
        call_kwargs = {
            **self.func_kwargs,
            **_build_laktory_context_kwargs(func, context),
        }
        return func(*self.func_args, **call_kwargs)
