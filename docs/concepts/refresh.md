A pipeline run either processes data on top of what its sinks already contain, or first resets
these sinks - their data and checkpoints - to reprocess everything from scratch. This page covers
how to select what a run does (`refresh`) and how sinks are reset (`reset_mode`).

## Run Modes

What a run does is selected with `refresh`:

| `refresh` | What the run does |
|---|---|
| `incremental` (default) | runs without resetting anything first: sinks are written according to their `mode` (e.g. `OVERWRITE` replaces the data, `APPEND` adds rows) and streaming sources resume from their checkpoint |
| `full` | resets the sinks of the selected nodes (data and checkpoints), then runs: all the data is reprocessed |
| `reset` | only resets the sinks of the selected nodes, without reading or writing data (see [Resetting Tables](#resetting-tables)) |

It's available wherever a pipeline is run:

- Python: `pl.execute(refresh="full")`
- `LAKEFLOW_JOB` orchestrator: the `refresh` job parameter (e.g. using *Run now with different
  parameters*)
- `AIRFLOW` orchestrator: the `refresh` DAG param

`refresh` replaces the `full_refresh` parameter of `pl.execute()`, the `full_refresh` job
parameter of the `LAKEFLOW_JOB` orchestrator and the `full_refresh` DAG param of the `AIRFLOW`
orchestrator: use `refresh="full"` instead.

## Reset Modes

`reset_mode` controls how a sink is reset:

- `reset_mode="DROP"` (default): drops the table entirely. It's recreated (schema and all) the
  next time the sink is written to.
- `reset_mode="TRUNCATE"`: empties the table - removes all rows, via an unconditional
  `DELETE FROM` since Delta does not support the `TRUNCATE TABLE` SQL statement - but keeps the
  table, its schema, its location and its grants intact.

It can be set at the sink, pipeline node, or pipeline level, or globally via
the `LAKTORY_RESET_MODE` environment variable / `settings.reset_mode` (see
[Laktory Settings](laktorysettings.md#reset-mode)). The value used for a sink is the first one set
among the sink, its pipeline node, its pipeline and the settings.

`TRUNCATE` is only supported for table sinks today; a `FileDataSink` only supports
`reset_mode="DROP"`.

To reset only part of a table - the rows written by a sink, when other writers (other nodes,
other pipelines, backfills, manual loads) also write to it - declare the rows owned by the sink
with `shared.where` (see [shared sinks](sharedsinks.md)): a full refresh then only deletes the
rows matching it, and `reset_mode` doesn't apply.

```yaml
sinks:
- schema_name: finance
  table_name: brz_stock_prices
  mode: APPEND
  shared:
    where: client_id = 'acme'
```

## Overriding the Reset Mode

The configured `reset_mode` can be overridden for a single run, e.g. to force a `DROP` after a
schema change on a sink configured with `TRUNCATE`:

- Python: `pl.execute(refresh="full", reset_mode="DROP")`
- `LAKEFLOW_JOB` orchestrator: the `reset_mode` job parameter, together with `refresh=full`
- `AIRFLOW` orchestrator: the `reset_mode` DAG param, together with `refresh=full`

The override requires `refresh` `full` or `reset`: it's rejected on an incremental run.

## Resetting Tables

A table may need to be reset as a whole, e.g. before a breaking schema change or after a data
corruption, by someone who can run the pipeline but not drop tables. Run the pipeline with
`refresh="reset"` and the `reset_mode` override: the tables written by the selected nodes are
reset (`DROP` or `TRUNCATE`), without reading or writing any data. The next run reprocesses all
the data.

- Lakeflow Job: *Run now with different parameters* with `refresh=reset` and `reset_mode=DROP`
  (optionally on a selection of tasks), then *Run now*.
- Python: `pl.execute(refresh="reset", reset_mode="DROP")`, then `pl.execute()`.

This works for every kind of sink, including [shared sinks](sharedsinks.md). Without the
override, a reset run resets each sink as a full refresh would (configured `reset_mode`, or the
writer's own rows for shared sinks).

## Shared Sinks

When a sink is written by multiple nodes or pipelines, a full refresh must not delete the data
of the other writers: each writer owns its rows - writer column or `shared.where` predicate (see
[shared sinks](sharedsinks.md)) - and a full refresh only deletes the rows of the writer. The `reset_mode` override resets the whole table
instead:

- `refresh="reset"` with the override: the table is reset as a whole, whichever writers are
  selected - a single writer task is enough - and the checkpoints of all its writers in the
  pipeline are reset. Other pipelines writing to the table need a full refresh.
- `refresh="full"` with the override: rejected for tables written by several nodes, as each
  writer would reset the table after the others wrote to it. Accepted for a table written by a
  single node of the pipeline (e.g. shared with other pipelines). Reset first, then run
  normally.

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipeline orchestrators, the declarative engine performs the
full refresh itself, clearing the tables and resetting their flows: `reset_mode` must stay `DROP`
(the default), and `refresh="reset"` is not supported.
