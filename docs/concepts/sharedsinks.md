??? "API Documentation"
    [`laktory.models.DataSinkSharedOptions`][laktory.models.DataSinkSharedOptions]<br>

A sink can be written by multiple writers: other nodes of the same pipeline and/or other
pipelines, e.g. several feeds pooled into one table, or one pipeline per client appending into a
cross-tenant table. With multiple writers, a [full refresh](refresh.md) must not delete the data
of the other writers, and the writers must not reset the table while others are writing to it.

Nodes of a pipeline writing to the same target are detected automatically and grouped. The
`shared` options, declared on every sink writing to the target, change this default.

## Configuration

```yaml
sinks:
- table_name: prices
  mode: APPEND
  shared:
    owner: node    # table (default) | pipeline | node
```

`owner` defines what a writer owns, which is what a full refresh deletes. It also defines how
the writers are executed:

| Scenario | `owner` | Executed as | Full refresh deletes |
|---|---|---|---|
| Several nodes of the pipeline write to the table | `table` (default, no `shared` needed) | a single task running all the writers | the whole table, once (`reset_mode`) |
| Other pipelines also write to the table | `pipeline` | a regular task (a single task if several nodes of the pipeline write to it) | this pipeline's rows |
| The writers must run independently (e.g. in parallel), whether or not other pipelines also write to the table | `node` | one task per writer | each writer's own rows |

## Grouped Writers

By default (`owner: table`), the writers of a target behave like a table with multiple append
flows in a declarative pipeline: they are executed together in one task - named
`shared-{table_name}`, or after their common `execution_task_name` - which resets the table once
and then runs them. Use `depends_on` to control their order, e.g. to have a node creating all the
columns run first.

```yaml
nodes:
- name: feed_b             # creates the table with all its columns
  sinks:
  - table_name: prices
    mode: APPEND
- name: feed_a             # fills some of the columns
  depends_on: [feed_b]
  sinks:
  - table_name: prices
    mode: APPEND
```

Selecting one of the writers (`selects`, or a task of a job run) always runs all of them. Rows of
a removed writer are gone after the next full refresh.

## Pipeline- and Node-Owned Rows

With `owner: pipeline`, other pipelines also write to the target; the nodes of the pipeline
writing to it are still grouped. With `owner: node`, each writer runs in its own task, possibly
in parallel. As rows owned by a node are also owned by its pipeline, other pipelines may write
to the target too. Both require a DELTA table or file sink in `APPEND` mode.

Each written row carries the writer identifier in a `_laktory_writer` column (first column,
configurable with `column`): `{pipeline_name}` with `owner: pipeline`,
`{pipeline_name}.{node_name}` with `owner: node` (overridable with `writer_id`). On a full
refresh, only the rows of the writer are deleted, so the data of the other writers is untouched
and the configured `reset_mode` is ignored (`reset_delete_where` is rejected).

Keep the identifiers stable:

- Rows of a removed or renamed node-owned writer, or of a decommissioned pipeline, are not
  deleted by any full refresh. Pin `writer_id` before renaming, or clean them up with
  `DELETE FROM <table> WHERE _laktory_writer = '<writer_id>'`.
- Rows written before a table's `owner` is set to `pipeline` or `node`, or before switching
  between them, don't carry the new writer identifier: drop the table once when converting.
- Parallel writers adding different columns conflict when evolving the table schema: declare the
  full `schema` on the sinks, or group the writers.

## Resetting Shared Tables

To reset a shared table as a whole, e.g. before a breaking schema change, run the pipeline with
`refresh="reset"` and the `reset_mode` override (see [Resetting Tables](refresh.md#resetting-tables)).
It works for every kind of shared sink. With `owner: pipeline` or `node`, the rows written by
other pipelines are reset too: these pipelines need a full refresh.

A full refresh with the `reset_mode` override is supported with `owner` `table` and `pipeline`
(the table is reset once), but rejected with `owner: node`, as the writers are executed
independently.

## Validation

Laktory validates shared sinks:

- writers of a same target in a pipeline must use the same `shared` options and, when grouped,
  the same `reset_mode` / `reset_delete_where`, as the table is reset once;
- pipelines of a same Stack writing to the same target must all declare `owner` `pipeline` or
  `node`;
- node-owned writers of a target must have distinct writer identifiers.

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipeline orchestrators, only grouped writers (`owner: table`)
are supported: the table is declared once as a streaming table and each node appends to it
through its own append flow, named `{table_name}__{node_name}`. The declarative engine runs the
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
