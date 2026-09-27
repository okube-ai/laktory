from typing import Literal

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field

from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild


class DataSinkSharedOptions(BaseModel, PipelineChild):
    """
    Options for a sink shared by multiple writers: other nodes of the same pipeline and/or
    other pipelines.

    `owner` defines what a writer owns, which is what a full refresh deletes:

    - `table` (default, same as no `shared` options): the pipeline owns the whole table. Nodes
      of the pipeline writing to the same sink are grouped in a single execution task and, on
      a full refresh, the table is reset once (according to `reset_mode`) before all writers
      reprocess their data.
    - `pipeline`: the pipeline owns its rows, other pipelines also write to the sink. Rows
      carry the pipeline identifier (`{pipeline_name}`) and, on a full refresh, only the rows
      written by this pipeline are deleted and reprocessed. Nodes of the pipeline writing to
      the sink are grouped in a single task.
    - `node`: each node owns its rows. Each writer runs in its own task (possibly in
      parallel), rows carry the writer identifier (`{pipeline_name}.{node_name}`) and, on a
      full refresh, a writer only deletes and reprocesses its own rows. Other pipelines may
      also write to the sink.

    Examples
    --------
    ```py
    import laktory as lk

    sink = lk.models.UnityCatalogDataSink(
        schema_name="finance",
        table_name="pooled_prices",
        mode="APPEND",
        shared={"owner": "pipeline", "writer_id": "client_a"},
    )
    print(sink.shared.uses_writer_column)
    # > True
    print(sink.shared.writer_id)
    # > client_a
    ```
    """

    owner: Literal["table", "pipeline", "node"] = Field(
        ...,
        description="""
        What a writer owns, and therefore what a full refresh deletes: the whole `table`
        (default when `shared` is not set), the rows of the `pipeline`, or the rows of the
        `node`. With `node`, each writer runs in its own task; rows of a removed or renamed
        node are no longer deleted on a full refresh.
        """,
    )
    writer_id_: str | None = Field(
        None,
        description="""
        Identifier stored in `column` for each written row, when `owner` is `pipeline` or
        `node`. Defaults to `{pipeline_name}` or `{pipeline_name}.{node_name}` respectively.
        Must be stable across runs: rows written with a previous identifier are no longer
        deleted on a full refresh.
        """,
        validation_alias=AliasChoices("writer_id", "writer_id_"),
        exclude=True,
    )
    column: str = Field(
        "_laktory_writer",
        description="Name of the column storing the writer identifier.",
    )

    @property
    def uses_writer_column(self) -> bool:
        """`True` if written rows carry the writer identifier."""
        return self.owner in ["pipeline", "node"]

    @computed_field(description="writer_id")
    @property
    def writer_id(self) -> str | None:
        if self.writer_id_ is not None:
            return self.writer_id_
        if not self.uses_writer_column:
            return None

        parents = [self.parent_pipeline]
        if self.owner == "node":
            parents += [self.parent_pipeline_node]
        names = [p.name for p in parents if p is not None and p.name]
        if not names:
            return None
        return ".".join(names)
