import importlib
from typing import Any

from pydantic import Field

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.typing import AnyFrame

logger = get_logger(__name__)

SUPPORTED_BACKENDS = [DataFrameBackends.PYSPARK]


class DataSinkForEachBatchOptions(BaseModel):
    """
    Options for writing a DataFrame using a custom `foreachBatch` function.

    Laktory manages the streaming query setup (trigger, checkpoint, start/await),
    while the user supplies the function that processes each micro-batch.

    Examples
    --------
    ```py
    from laktory import models

    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        mode="FOREACH_BATCH",
        foreach_batch_options={
            "func": "mypackage.etl.process_batch",
        },
    )
    # sink.write(streaming_df)
    ```

    The user-supplied function must have the signature:

    ```py
    def process_batch(batch_df, batch_id: int) -> None:
        batch_df.write.format("delta").mode("append").saveAsTable("my_table")
    ```

    References
    ----------
    * [Structured Streaming foreachBatch](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch)
    """

    func: str = Field(
        ...,
        description="Fully qualified dotted path to a callable that processes each micro-batch. "
        "Example: 'mypackage.etl.process_batch'. "
        "Function signature must be: func(batch_df, batch_id: int) -> None.",
    )
    _parent: Any = None

    # ----------------------------------------------------------------------- #
    # Sink                                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def sink(self):
        return self._parent

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _resolve_func(self):
        """Dynamically import and return the callable from its dotted path."""
        module_path, func_name = self.func.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)

    def execute(self, source: AnyFrame):
        """
        Execute the custom foreachBatch function against the source DataFrame.

        For streaming DataFrames, Laktory wraps the function in
        `writeStream.foreachBatch(...).trigger(availableNow=True).options(checkpointLocation=...).start()`
        and awaits termination.

        For batch DataFrames, the function is called directly as
        `func(source.to_native(), 0)`.

        Parameters
        ----------
        source:
            Source DataFrame to process.
        """
        dataframe_backend = DataFrameBackends(source.implementation)
        if dataframe_backend not in SUPPORTED_BACKENDS:
            raise NotImplementedError(
                f"DataFrame provided is of {dataframe_backend} backend, which is not currently implemented for foreachBatch operations."
            )

        func = self._resolve_func()
        logger.info(f"Resolved foreachBatch function: {self.func}")

        source = source.to_native()

        if source.isStreaming:
            if self.sink and self.sink.checkpoint_path is None:
                raise ValueError(
                    f"Checkpoint location not specified for sink '{self.sink}'"
                )

            query = (
                source.writeStream.foreachBatch(func)
                .trigger(availableNow=True)
                .options(
                    checkpointLocation=self.sink.checkpoint_path,
                )
                .start()
            )
            query.awaitTermination()

        else:
            func(source, 0)
