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

    Nodes of a pipeline writing to the same sink target are detected automatically: by
    default, they are grouped in a single execution task and, on `full_refresh`, the table is
    reset once (according to `full_refresh_mode`) before all writers reprocess their data.
    These options are only required to change this default:

    - `isolated`: each writer runs in its own task (possibly in parallel) and rows carry the
      writer identifier (`{pipeline_name}.{node_name}`). On `full_refresh`, a writer only
      deletes and reprocesses its own rows.
    - `external`: other pipelines also write to the sink. Rows carry the pipeline identifier
      (`{pipeline_name}`) and, on `full_refresh`, only the rows written by this pipeline are
      deleted and reprocessed, leaving the data of other pipelines untouched.

    Examples
    --------
    ```py
    import laktory as lk

    sink = lk.models.UnityCatalogDataSink(
        schema_name="finance",
        table_name="pooled_prices",
        mode="APPEND",
        shared={"external": True, "writer_id": "client_a"},
    )
    print(sink.shared.uses_writer_column)
    # > True
    print(sink.shared.writer_id)
    # > client_a
    ```
    """

    internal: bool = Field(
        False,
        description="""
        Other nodes of the same pipeline also write to this sink. Optional, for documentation
        purposes only: writers of a same sink target are detected automatically.
        """,
    )
    external: bool = Field(
        False,
        description="Other pipelines also write to this sink.",
    )
    isolated: bool = Field(
        False,
        description="""
        If `True`, the node writes on its own: it runs in its own task and `full_refresh` only
        deletes and reprocesses its own rows. Rows of a removed or renamed node are no longer
        deleted on `full_refresh`. If `False`, the writers of the sink within the pipeline are
        grouped in a single task.
        """,
    )
    writer_id_: str | None = Field(
        None,
        description="""
        Identifier stored in `column` for each written row, when a writer column is used
        (`external` or `isolated`). Defaults to `{pipeline_name}.{node_name}` if `isolated`,
        `{pipeline_name}` otherwise. Must be stable across runs: rows written with a previous
        identifier are no longer deleted on `full_refresh`.
        """,
        validation_alias=AliasChoices("writer_id", "writer_id_"),
        exclude=True,
    )
    column: str = Field(
        "_laktory_writer",
        description="Name of the column storing the writer identifier.",
    )

    @model_validator(mode="after")
    def validate_flags(self) -> Any:
        if not (self.internal or self.external or self.isolated):
            raise ValueError(
                "`shared` requires at least one of `internal`, `external` or `isolated` to be "
                "`true`. Nodes of a pipeline writing to the same sink don't need `shared`: "
                "they are grouped automatically."
            )
        return self

    @property
    def uses_writer_column(self) -> bool:
        """`True` if written rows carry the writer identifier."""
        return self.external or self.isolated

    @computed_field(description="writer_id")
    @property
    def writer_id(self) -> str | None:
        if self.writer_id_ is not None:
            return self.writer_id_

        parents = [self.parent_pipeline]
        if self.isolated:
            parents += [self.parent_pipeline_node]
        names = [p.name for p in parents if p is not None and p.name]
        if not names:
            return None
        return ".".join(names)
