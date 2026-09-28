A pipeline run either processes new data on top of what its sinks already contain, or first
resets its sinks - data and checkpoints - to reprocess everything. `refresh` selects what a run
does, `reset_mode` how sinks are reset.

## Run Modes

| `refresh` | What the run does |
|---|---|
| `INCREMENTAL` (default) | writes the sinks according to their `mode` (`APPEND` adds rows, `OVERWRITE` replaces them); streaming sources resume from their checkpoint |
| `FULL` | resets the sinks of the selected nodes, then runs: all the data is reprocessed |
| `RESET` | only resets the sinks of the selected nodes, without reading or writing data |

`RESET` resets both the data and the checkpoints of the sinks. It differs from the Databricks
pipelines `reset_checkpoint_selection` option, which only resets the checkpoints of streaming
flows and keeps the data.

Set it wherever a pipeline runs:

| Where | How |
|---|---|
| Python | `pl.execute(refresh="FULL")`, or `node.execute(refresh="FULL")` for a single node |
| CLI | `laktory run --databricks-job <job> --refresh FULL` |
| `LAKEFLOW_JOB` | `refresh` job parameter (*Run now with different parameters*) |
| `AIRFLOW` | `refresh` DAG param |

Run parameters (`refresh`, `reset_mode`) are case-insensitive: `refresh=full` works too.

`refresh="FULL"` replaces `full_refresh=True`. A run still passing `full_refresh=true` fails
instead of running incrementally: `pl.execute(full_refresh=True)`,
`node.execute(full_refresh=True)`, or a job or Airflow run (e.g. a job deployed before 0.13.0, or
an existing trigger) - redeploy the job and update the trigger. `full_refresh=false` logs a
warning and runs incrementally.

## Reset Modes

| `reset_mode` | Effect |
|---|---|
| `DROP` (default) | drops the table; it's recreated on the next write |
| `TRUNCATE` | deletes all rows, keeping the table, its schema, location and grants |

`TRUNCATE` is supported by:

| Sink | How |
|---|---|
| table (not views) | `DELETE FROM <table>` |
| DELTA file | `DELETE FROM delta.<path>`: also keeps the table identity (downstream streams keep reading it), history and properties |

`reset_mode` is set on each sink, since it depends on the table (grants, downstream consumers,
schema changes):

```yaml
name: pl-stocks
nodes:
- name: gld_prices
  sinks:
  - table_name: gld_prices
    reset_mode: TRUNCATE    # keep the table and its grants
```

- On other sinks (views, PARQUET / CSV / JSON / ... files, declarative pipelines), `TRUNCATE` is
  rejected; passed as a run override, the sink falls back to `DROP` (logged).
- To use the same value for many sinks, use a [variable](variables.md) (e.g.
  `reset_mode: ${vars.reset_mode}`).
- To reset only part of a table, declare the rows the sink owns with `shared.where` (see
  [Shared Sinks](sharedsinks.md#identifying-the-rows-of-a-writer)).

## Overriding the Reset Mode

Override `reset_mode` for a single run, e.g. to drop a table configured with `TRUNCATE` after a
schema change:

| Where | How |
|---|---|
| Python | `pl.execute(refresh="FULL", reset_mode="DROP")` |
| CLI | `laktory run --databricks-job <job> --refresh FULL --reset-mode DROP` |
| `LAKEFLOW_JOB` | `reset_mode` job parameter |
| `AIRFLOW` | `reset_mode` DAG param |

The override requires `refresh` `FULL` or `RESET`.

## Resetting a Table

To reset a table before a breaking schema change or after a data corruption - e.g. by someone who
can run the pipeline but not drop tables - run with `refresh=RESET`, then normally:

- Lakeflow Job: *Run now with different parameters* with `refresh=RESET` and `reset_mode=DROP`
  (optionally on a selection of tasks), then *Run now*.
- CLI: `laktory run --databricks-job <job> --refresh RESET --reset-mode DROP` (optionally with
  `--tasks`), then `laktory run --databricks-job <job>`.
- Python: `pl.execute(refresh="RESET", reset_mode="DROP")`, then `pl.execute()`.

## Shared Sinks

A [shared sink](sharedsinks.md) is written by several nodes or pipelines, each owning its rows. A
full refresh of a writer only deletes its own rows, and `reset_mode` doesn't apply - unless it's
overridden:

| Run | Regular sink | Shared sink |
|---|---|---|
| `FULL` | reset (`reset_mode`), reprocess | delete the writer's rows, reprocess |
| `RESET` | reset (`reset_mode`) | delete the writer's rows |
| `RESET` + override | reset (override) | reset the whole table (override), from any writer's task |
| `FULL` + override | reset (override), reprocess | rejected: a full refresh never deletes the rows of the other writers |

See [Resetting a Shared Table](sharedsinks.md#resetting-a-shared-table).

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipelines, the engine performs the full refresh itself: it
clears the tables and resets their flows. `reset_mode` set on their sinks must be `DROP`, and `refresh="RESET"` is not supported.
