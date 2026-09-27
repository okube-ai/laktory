??? "API Documentation"
    [`laktory.models.DataSinkSharedOptions`][laktory.models.DataSinkSharedOptions]<br>

A sink can be written by multiple writers: other nodes of the same pipeline and/or other
pipelines, e.g. several feeds pooled into one table, several nodes sending their quarantined rows
to the same table, or one pipeline per client appending into a cross-tenant table. With multiple
writers, a [full refresh](refresh.md) of one writer must not delete the data of the others.

Laktory solves this with row ownership: each writer owns its rows - identified by a writer
column or by a SQL predicate - and a full refresh of a writer only deletes and reprocesses its
own rows. Each writer can
therefore be executed and refreshed independently - in any order, in parallel, alone or with
others, from a job run of any selection of tasks - without coordination between writers.

## Several Nodes of a Pipeline

Nodes of a pipeline writing to the same target are detected automatically: each node owns its
rows, no configuration needed.

```yaml
nodes:
- name: feed_a
  sources:
  - node_name: brz_prices
  sinks:
  - table_name: prices
    mode: APPEND
- name: feed_b
  sources:
  - node_name: brz_prices
  sinks:
  - table_name: prices
    mode: APPEND
```

- Each row carries its writer in a `_laktory_writer` column (first column):
  `{pipeline_name}.{node_name}`, e.g. `pl-prices.feed_a`.
- Each writer runs in its own task (`node-{name}`, or its `execution_task_name`), possibly in
  parallel with the others.
- A full refresh of `feed_b` (alone or with others) deletes the rows of `feed_b` and reprocesses
  its data. The rows of `feed_a` are untouched.
- The writers must be DELTA table or file sinks in `APPEND` mode. Other modes (`OVERWRITE`,
  `MERGE`) or formats can't identify the rows of a writer: write to separate tables instead.
- The configured `reset_mode` doesn't apply on a full refresh: each writer deletes its own rows.

This is the same as declaring `shared: true` on each sink. `shared` options can customize how
the rows of a writer are identified (see below).

## Several Pipelines

A table written by several pipelines requires `shared` options on every sink writing to it, so
that a full refresh of a pipeline only deletes its own rows. A pipeline can't detect it by
itself - other pipelines may be deployed separately:

```yaml
sinks:
- table_name: all_orders
  mode: APPEND
  shared: true    # rows tagged `{pipeline_name}.{node_name}`
```

The pipeline name in the writer identifier keeps the rows of the pipelines apart, whatever
their number of nodes. In a Stack, a table written by several pipelines without `shared` options
on all its sinks raises a validation error. Pipelines using a declarative orchestrator can't
share a table with other pipelines.

## Identifying the Rows of a Writer

By default, each row carries its writer in a writer column. The `shared` options customize it:

| Option | Default | Description |
|---|---|---|
| `writer_id` | `{pipeline_name}.{node_name}` | identifier of the writer, stored in the writer column |
| `column` | `_laktory_writer` | name of the writer column |
| `where` | - | SQL predicate matching the rows of the writer, used instead of a writer column |

To avoid adding a column, e.g. when the writers of a table already write distinct values of a
business key, declare the rows each writer owns with `where`. It also applies to a single
writer, when other processes (backfills, manual loads) write to the same table: a full refresh
only deletes the rows matching the predicate. Laktory logs the number of deleted rows: check it
to catch a wrong or stale predicate.

```yaml
# pl-client-23.yaml
sinks:
- table_name: all_orders
  mode: APPEND
  shared:
    where: client_id = 23    # full refresh: DELETE ... WHERE client_id = 23
```

- All the writers of a table must identify their rows the same way: either all with a writer
  column (including tables detected automatically), or all with `where`. A predicate could
  otherwise match the rows of the other writers.
- The predicates of the writers must not overlap, and each writer must only write rows matching
  its predicate: rows it writes outside of it are not deleted by its full refresh. Laktory
  rejects identical predicates, but can't check that they don't overlap.

## Writer Identifiers

Keep the identifiers stable:

- Rows of a removed or renamed writer, or of a decommissioned pipeline, are not deleted by any
  full refresh. Pin `writer_id` before renaming, clean them up with
  `DELETE FROM <table> WHERE _laktory_writer = '<writer_id>'`, or reset the whole table (see
  below).
- A table written before being shared with a writer column (e.g. by a single node, before a
  second node writes to it) has no `_laktory_writer` column. Writing to it, or refreshing a writer, fails with an error
  asking to drop it once: run with `refresh="reset"` and `reset_mode="DROP"`, then normally.
- When switching a table from a writer column to `where` predicates (or the reverse), drop it
  once (`refresh="reset"`, `reset_mode="DROP"`): its existing rows are not identified by the new
  ownership.
- Parallel writers adding different columns conflict when evolving the table schema: declare the
  full `schema` on the sinks, or use `depends_on` to have the node creating all the columns run
  first.

## Resetting Shared Tables

To drop or empty a shared table as a whole, e.g. before a breaking schema change or to clean up
the rows of removed writers, run with `refresh="reset"` and the `reset_mode` override (see
[Resetting Tables](refresh.md#resetting-tables)):

- Lakeflow Job: *Run now with different parameters* with `refresh=reset` and `reset_mode=DROP`
  (or `TRUNCATE`), on the whole job or on any selection of tasks - a single writer task is enough.
- Python: `pl.execute(refresh="reset", reset_mode="DROP")`.

The table is reset as a whole, whichever writers are selected, and the checkpoints of all its
writers in the pipeline are reset too: on their next run, they reprocess all their data. The
other pipelines writing to the table need a full refresh.

A full refresh (`refresh="full"`) with the `reset_mode` override is rejected for tables written by
several nodes: each writer would reset the table after the others wrote to it. Reset the table
first, then run normally.

## Validation

Laktory validates shared sinks:

- writers of a same target must be DELTA sinks in `APPEND` mode;
- pipelines of a same Stack writing to the same target must all declare `shared` options, and
  none of them may use a declarative orchestrator;
- writers of a target must identify their rows the same way (writer column or `where`), with the
  same `column`, and distinct writer identifiers or predicates.

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipeline orchestrators, the declarative engine owns the
tables, so `shared` options are not supported and no writer column is added: a table written by
several nodes is declared once as a streaming table and each node appends to it through its own
append flow, named `{table_name}__{node_name}`. The declarative engine runs the
flows in parallel and handles the full refresh itself, clearing the table once and resetting
every flow. A table written by a declarative pipeline can't be shared with other pipelines. The
following rules apply:

- All sinks must be streaming, non-CDC (`MERGE`) table sinks.
- Table properties (`comment`, `table_properties`, `format`) can be declared on any of the sinks,
  but must not conflict.
- With Lakeflow Declarative Pipelines, expectations are applied to the whole table (append flows
  don't support them), so all nodes writing to it must declare the same expectations.

A flow checkpoint is identified by its name. Adding a second node to an existing single-writer
streaming table (or renaming a node) changes the flow name of the existing writer, which then
reprocesses its source from scratch - run a full refresh of that table once after the change.
