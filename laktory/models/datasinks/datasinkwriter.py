import importlib
from typing import Any

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


class DataSinkWriter(BaseModel, PipelineChild):
    """
    Definition of a custom write function to be called when writing a DataFrame
    to a sink. Gives the user full control over how data is written.

    Laktory manages the streaming query lifecycle (foreachBatch wrapping, trigger,
    checkpoint, start/await) while the user-supplied function handles the actual
    write logic.

    The function is called as:

    ```python
    func(df, *func_args, node=node, **func_kwargs)
    ```

    where `node` is the parent `PipelineNode` (or `None` when the sink is used
    outside a pipeline) and `df` is the DataFrame (native or Narwhals depending
    on the sink's `dataframe_api` setting).

    Examples
    --------
    ```py
    from laktory import models

    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        write_func={
            "func_name": "mypackage.etl.my_write",
            "func_kwargs": {"extra_tag": "production"},
        },
    )
    # sink.write(df)
    ```

    ```py
    # mypackage/etl.py
    def my_write(df, extra_tag="default", node=None) -> None:
        # Optionally use node context to avoid re-hardcoding sink coordinates
        # sink = node.primary_sink
        df.write.format("delta").mode("append").save("./my_table/")
    ```
    """

    func_args: list[Any] = Field(
        [],
        description="Positional arguments passed to the function after the DataFrame.",
    )
    func_kwargs: dict[str, Any] = Field(
        {},
        description=(
            "Keyword arguments passed to the function. "
            "The key 'node' is reserved and injected automatically by Laktory "
            "with the parent PipelineNode instance."
        ),
    )
    func_name: str = Field(
        ...,
        description=(
            "Fully qualified dotted path to the callable. "
            "Example: 'mypackage.etl.my_write'."
        ),
    )

    @model_validator(mode="after")
    def validate_reserved_keys(self):
        if "node" in self.func_kwargs:
            raise ValueError(
                "'node' is a reserved key in `func_kwargs` and is injected "
                "automatically by Laktory. Remove it from `func_kwargs`."
            )
        return self

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _resolve_func(self):
        """Dynamically import and return the callable from its dotted path."""
        module_path, func_name = self.func_name.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)

    def execute(self, df, node=None) -> None:
        """
        Invoke the user-supplied function with the DataFrame, configured
        args/kwargs, and the pipeline node context.

        Parameters
        ----------
        df:
            Input DataFrame (native or Narwhals, as prepared by the caller).
        node:
            Parent PipelineNode, or None when the sink is used standalone.
        """
        func = self._resolve_func()

        if not isinstance(df, (nw.DataFrame, nw.LazyFrame)):
            df = nw.from_native(df)

        if self.dataframe_api == "NATIVE":
            df = nw.to_native(df)

        func_log = f"{self.func_name}("
        func_log += ",".join(self.func_args)
        func_log += ",".join([f"{k}={v}" for k, v in self.func_kwargs.items()])
        func_log += f") with df type {type(df)}"
        logger.info(f"Writing df with {func_log}")

        func(df, *self.func_args, node=node, **self.func_kwargs)
