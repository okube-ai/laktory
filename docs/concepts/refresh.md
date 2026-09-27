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
- `reset_mode="DELETE_WHERE"`: deletes only the rows matching a `reset_delete_where` SQL
  predicate, leaving every other row untouched. For tables written by multiple pipelines, prefer
  [shared sinks](sharedsinks.md), which track row ownership automatically.

```yaml
sinks:
- schema_name: finance
  table_name: brz_stock_prices
  reset_mode: DELETE_WHERE
  reset_delete_where: client_id = 'acme'
```

`DROP` and `TRUNCATE` can be set at the sink, pipeline node, or pipeline level, or globally via
the `LAKTORY_RESET_MODE` environment variable / `settings.reset_mode` (see
[Laktory Settings](laktorysettings.md#reset-mode)). The value used for a sink is the first one set
among the sink, its pipeline node, its pipeline and the settings.

`reset_delete_where` requires DELTA format and must be set directly on the sink that owns the
predicate - it is not inherited from a parent pipeline node, pipeline, or global setting, since a
deletion predicate is inherently specific to one sink. `reset_mode="DELETE_WHERE"` follows the
same rule: it can only be set directly on a sink, and raises a validation error if set on a
`PipelineNode`, `Pipeline`, or globally. It's not supported on [shared sinks](sharedsinks.md) with
`owner` `pipeline` or `node`, whose writer column identifies the rows to delete. Grouped writers
of a table must use the same `reset_mode` and `reset_delete_where`, as the table is reset once.

Laktory logs the number of rows deleted by `reset_delete_where`: check it in the run logs to catch
a wrong or stale predicate.

`TRUNCATE`/`DELETE_WHERE` are only supported for table sinks today; a `FileDataSink` only
supports `reset_mode="DROP"`.

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
of the other writers: depending on its [shared sink](sharedsinks.md) options, the table is
reset once for all the writers of a pipeline, or only the rows of the writer are deleted. The
`reset_mode` override then applies as follows:

- `owner: table` (grouped writers): the table is reset once, with the override.
- `owner: pipeline`: the whole table is reset once, including the rows written by other
  pipelines, which then need a full refresh too.
- `owner: node`: a full refresh with the override is rejected, as the writers are executed
  independently. Run with `refresh="reset"` and the override first, then with
  `refresh="full"`.

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipeline orchestrators, the declarative engine performs the
full refresh itself, clearing the tables and resetting their flows: `reset_mode` must stay `DROP`
(the default), and `refresh="reset"` is not supported.
