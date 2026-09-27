from typing import Any

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild


class DataSinkSharedOptions(BaseModel, PipelineChild):
    """
    Options for a sink shared by multiple writers: other nodes of the same pipeline and/or
    other pipelines.

    Each writer owns its rows, and a full refresh of a writer only deletes and reprocesses
    its own rows, leaving the data of the other writers untouched. Each writer can therefore
    be executed and refreshed independently, in any order, possibly in parallel. The rows of
    a writer are identified either by:

    - a writer column (default): each written row carries the identifier of its writer in
      `column`, `{pipeline_name}.{node_name}` by default;
    - a SQL predicate (`where`), e.g. `client_id = 23`, matching the rows written by the
      writer: no column is added.

    Applied automatically, with a writer column, when several nodes of a pipeline write to the
    same sink. `shared: true` is equivalent to `shared: {}`.

    Examples
    --------
    ```py
    import laktory as lk

    sink = lk.models.UnityCatalogDataSink(
        schema_name="finance",
        table_name="pooled_prices",
        mode="APPEND",
        shared={"writer_id": "client_a"},
    )
    print(sink.shared.writer_id)
    # > client_a

    sink = lk.models.UnityCatalogDataSink(
        schema_name="finance",
        table_name="pooled_prices",
        mode="APPEND",
        shared={"where": "client_id = 23"},
    )
    print(sink.shared.uses_writer_column)
    # > False
    ```
    """

    writer_id_: str | None = Field(
        None,
        description="""
        Identifier stored in `column` for each written row. Defaults to
        `{pipeline_name}.{node_name}`. Must be stable across runs: rows written with a
        previous identifier are no longer deleted on a full refresh.
        """,
        validation_alias=AliasChoices("writer_id", "writer_id_"),
        exclude=True,
    )
    column: str = Field(
        "_laktory_writer",
        description="Name of the column storing the writer identifier.",
    )
    where: str | None = Field(
        None,
        description="""
        SQL predicate matching the rows owned by the writer, e.g. `client_id = 23`, used
        instead of a writer column: a full refresh deletes the rows matching it. The
        predicates of the writers of a sink must not overlap, and each writer must only
        write rows matching its predicate.
        """,
    )

    @model_validator(mode="after")
    def where_excludes_writer_column(self) -> Any:
        if self.where is not None and (
            self.writer_id_ is not None or self.column != "_laktory_writer"
        ):
            raise ValueError(
                "`shared.where` identifies the rows of the writer without a writer column: "
                "`writer_id` and `column` can't be set with it."
            )
        return self

    @property
    def uses_writer_column(self) -> bool:
        """`True` if written rows carry the writer identifier in `column`."""
        return self.where is None

    @computed_field(description="writer_id")
    @property
    def writer_id(self) -> str | None:
        if self.writer_id_ is not None:
            return self.writer_id_
        if not self.uses_writer_column:
            return None

        parents = [self.parent_pipeline, self.parent_pipeline_node]
        names = [p.name for p in parents if p is not None and p.name]
        if len(names) < 2:
            return None
        return ".".join(names)
